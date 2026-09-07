# Wave 9 upstream issues — `Saurabh17111994/go-arrow` (7 issues, paste-ready)

Upstream: `github.com/Saurabh17111994/go-arrow`, pinned `go-bridge/go.mod:11`
`v0.0.0-20260622-7cce1630` + `replace => ./third_party/go-arrow`.
`git ls-remote HEAD == 7cce1630ae2d45c5...` == pin (verified 2026-09-08, zero drift;
vendored = HEAD + local R-101..R-243). Policy: pin-only, never hand-edit vendored —
these issues are the work; downstream re-pins on tag, then `go test ./...` + `-race`
trio + `vet` + static guards, then ticks 65.

Bundle spec: `docs/plans/2026-09-08-wave9-upstream-bundles.md` A–G.
Downstream evidence: P1 `OPEN=66 / CLOSED=254 TOTAL=320`, WAVE 9 `[ ]` OPEN
(65 AGREE open + P1-177 PARTIAL).

---

## Issue 1 — A: `arrow/client.go` token race (P1-031/035 + DUP 032/033)

**Title:** `Client` token/config reads race `SetToken` — add RWMutex coverage

**Body:**

`Client.mu sync.RWMutex` exists (`arrow/client.go:34`) and `SetDebug`/`IsDebug`
(`:203-212`) use it, but the token path does not:

- `SetToken` (`:197-199`) writes `c.Config.Token` with no lock.
- `GetToken` (`:232-234`) and `GetRefreshToken` (`:242-244`) read `c.Config`
  with no lock.
- `request` (`:77-87`), `connectHFTDataStreamURL` (`:64-67`) read
  `BaseURL`/`AppID`/`Token` with no lock.

Any concurrent refresh-vs-request (our bridge refreshes auth while requests
fly) is a data race.

**Fix required:**

1. `Lock` in `SetToken`; `RLock` in `GetToken`/`GetRefreshToken`/`IsDebug`.
2. In `request`/`requestWith`/`connectHFTDataStreamURL`: copy
   `BaseURL`/`AppID`/`Token` under `RLock` before building the fasthttp
   request (narrows P1-035 read side and DUP P1-032/033 to the same lock).

**Repro (downstream):**

```sh
PROVE_VENDORED_RACE=1 go test -race -run TestVendoredTokenRaceRepro .
# fires on third_party/go-arrow/arrow/client.go:198 vs :233
# captured /tmp/w9race.txt 2026-09-08
```

Downstream shield (`bridgeAuthMu`, own-side only) does not fix the SDK;
this issue is the real fix. Closes P1-031/035 + DUP 032/033.

---

## Issue 2 — B: `arrow/auth.go` auth contract (P1-161/163/164/165/166/167/292/293/294)

**Title:** Auth contract: dual checksum key, blocking `Login`, untyped `AutoLogin` errors

**Body:**

- `Authenticate` (`arrow/auth.go:63-70`) sends both `CheckSum` and `checksum`
  with the same value. One must be canonical; dual keys break strict servers
  and confuse readers (P1-293).
- `Login` (`:115-135`) owns stdin (`fmt.Scanln :123`, no timeout). Any caller
  in a service hangs forever (P1-162; downstream is AutoLogin-only, zero
  `.Login(` callers repo-wide — static guard
  `code/01_platform/04_scripts/tests/test_vendored_auth_wave9.py` 2/2 green).
- `AutoLogin` (`:150-247`) returns untyped errors (bad-creds vs bad-TOTP vs
  network indistinguishable, P1-163/164), prints a token-length side channel
  (P1-292).
- `GenerateChecksum` (`:41-45`) accepts an empty secret.

**Fix required:**

1. Pick one checksum key; drop the other after broker confirms canonical.
2. Document stdin ownership on `Login`; add `LoginContext(ctx)` variant
   (timeout/cancel).
3. `AutoLogin`: typed `AuthError` (bad-creds vs bad-TOTP vs network); stop
   printing token length.
4. `GenerateChecksum`: reject empty secret.
5. `Authenticate`: validate non-empty `requestToken` (P1-161); wrap
   marshal/do/unmarshal errors with endpoint context (P1-166).

**Broker unknown blocking (1):** canonical checksum key — `CheckSum` or
`checksum`? (`auth.go:64-65` sends both.) Need Arrow docs/support answer
before dropping one.

---

## Issue 3 — C: `arrow/hft_stream.go` subscribe validation (P1-037/038/039/176/178 + PARTIAL P1-177)

**Title:** HFT subscribe: no `exchSeg`/`latency` range checks, map-order `symIds`, nil-receiver lock order

**Body:**

Constants (`arrow/hft_stream.go:22-25`): `0=NSECM, 1=NSEFO, 2=BSECM, 3=BSEFO`.

- `SubscribeHFTTokens` (`:216-235`) validates mode + non-empty ids only — no
  `exchSeg 0..3` range, no `latencyMs` bounds. Invalid segment goes on the
  wire as `{"exch_seg": exchSeg, ...}` (`:231-233`).
- `SubscribeHFTBySegment` (`:239-263`) ranges a Go map — `symIds` order is
  nondeterministic across identical calls.
- `decodeHFTPayload` (`:97-104`) takes `s.zdecMu.RLock()` *before* the
  `s == nil` check — nil receiver panics instead of returning payload.
- `hftPacketMeta` (`:109-128`) has no length-gate before dispatch (P1-039).
- Downstream pin (not a fix): `go-bridge/main.go:540` passes constant
  `HFTExchNSECM`; latency via `hftRange` + `SlotConfig.Validate 50..60000`
  (`go-bridge/hft_slot.go:55-74`, mode/latency/timings only).
  Guard `TestSubscribeParamsPinned` pins our constant — range+sort stays here.

**Fix required:**

1. Range-check `exchSeg` (0..3 or broker-confirmed table) and `latencyMs`
   bounds in `SubscribeHFTTokens`.
2. Sort segment keys in `SubscribeHFTBySegment`.
3. Reorder nil check before lock in `decodeHFTPayload`.
4. Single-reader `atomic.Bool` in `ReadHFTWithFrame` (second caller → error).
5. Length-gate before `hftPacketMeta` dispatch; `normalizeHFTMode` error names
   the valid set (P1-176); wrap `writeJSON` deadline error with op context
   (P1-178).

**Broker unknowns blocking (2):** `exch_seg` table — is `0..3` exhaustive or
do MCX/NCD/BCD exist? `latencyMs` legal min/max — broker bounds vs our
`50..60000` clamp?

---

## Issue 4 — D: `arrow/streams.go` deadlines (P1-045/046/047/199/200/202/302 + DUP 201)

**Title:** Data/order streams: bare handshake errors, no read-deadline, indefinite subscribe-ack

**Body:**

- `ConnectDataStream` (`arrow/streams.go:79-89`) and `sendSubMessage`
  (`:103-112`) return bare handshake/write errors with no URL/op context.
- The `ReadMessage` loop has no read-deadline (HFT got one via R-103; this
  loop needs the same treatment — P1-200 + DUP 201).
- Subscribe-ack waits indefinitely instead of failing closed (P1-202).
- Keepalive undocumented/unconfigurable (P1-047); `OrderUpdate` doc field
  names stale (P1-302).

**Fix required:**

1. Wrap `connectDataStreamURL`/`connectOrderStreamURL` errors with URL
   context minus secrets (P1-045/046).
2. Document or parameterize keepalive (P1-047).
3. Nil-conn guard (P1-199); read-deadline on the `ReadMessage` loop (P1-200).
4. Subscribe-ack timeout instead of indefinite wait (P1-202).
5. Fix `OrderUpdate` doc field names (P1-302).

**Broker unknowns blocking (2):** keepalive interval broker expects on
`wss://ds.arrow.trade` + `wss://socket.arrow.trade`? Subscribe-ack SLA /
read-deadline ms for fail-closed value?

---

## Issue 5 — E: numeric types, float money (P1-040/044/196/197/198/299) — BREAKING

**Title:** [BREAKING] float64/string money → integer paise or documented decimal

**Body:**

`margin.go`, `positions.go`, `quote.go` carry money as float64/string.
Float money loses paise on large notionals; string money pushes parsing +
overflow risk onto every caller. Downstream converts at its boundary today,
but the SDK type is the hazard.

**Fix required:** integer paise or documented decimal with overflow checks
across margin/positions/quote. This is a **breaking** wire-type change —
ship as a major bump with a migration note.

**Broker unknown blocking (1):** paise-int or decimal — which does Arrow
canonicalize on? Downstream should not guess.

---

## Issue 6 — F: parse validation (P1-041/042/185-193/194/195/179-184/203-205/295-301 + MITIGATED 036/043/172)

**Title:** Parse layer accepts unknown enums, GTC/INDEX, empty `OrderNo`

**Body:**

- `GenericResponse` (`arrow/market.go:12-15`) zero-accepts unknown
  `status`/`appResponse` values (P1-041/042).
- `OrderRequest` (`arrow/orders.go:13-26`) leaves `Validity` a free string;
  `ValidityGTC` (`arrow/constants.go:61`) is advertised but the broker path
  takes DAY|IOC only (P1-036 — downstream `06_execution_bridge/.../models.go:174-177`
  rejects; guard `TestValidateCommandRejectsGTCValidity` green).
  `ExchangeINDEX` (`constants.go:17`) is order-reachable in-SDK but not
  executable (P1-172 — downstream `models.go:143` rejects; guard green).
- `PlaceOrder` (`arrow/orders.go:178-194`) returns success with empty
  `Data.OrderNo` (P1-043 — downstream `broker.go:62-63,81-82` rejects to
  UNKNOWN; guard `TestBridgeAcceptanceRequiresOrderNo` green).
- Triplicated request helper (`client.go` — one `doRequest`, P1-295/296 DUP).
- Per-endpoint HTTP status mapping missing (P1-185-188); `price`/`qty <= 0`
  unrejected (P1-191); holdings/limits/margin/user/quote/doc nits
  (P1-179/180/181/182/183/184/203/204/205/298/300/301/297).

**Fix required:**

1. Reject unknown `appResponse` values instead of zero-value accept.
2. `orders.go`: `OrderNo` empty check; `Validity`/`Exchange` enums reject
   GTC/INDEX; `price`/`qty > 0`.
3. One `doRequest` helper; per-endpoint HTTP status mapping.
4. `doc.go`: correct import path + TOTP-only auth (P1-297).

**Broker unknowns blocking (3):** full `status`/`appResponse` enum; does the
broker ever return `success` + empty `orderNo`? Does it reject `GTC`/`INDEX`
server-side? Per-endpoint HTTP status map.

---

## Issue 7 — G: repo hygiene (P1-030/159/160)

**Title:** `.gitignore`: `Makefile` ignored, `.env` risk, `*.log` too broad

**Body:**

- Ignoring `Makefile` is unusual for a vendored tree and harms
  reproducibility (P1-030).
- No committed `.env` today, but no `config.example` either — rotation path
  undocumented (P1-159).
- `*.log` at any depth matches more than build output (P1-160).

**Fix required:** track `Makefile` deliberately or document why ignored;
confirm `.env` never committed + add `config.example`; narrow `*.log` to
build dirs.

---

## Broker unknowns consolidated (7 — only Arrow docs/support can settle)

1. Canonical checksum key — `CheckSum` or `checksum`? (B)
2. `exch_seg` table — `0..3` exhaustive, or MCX/NCD/BCD exist? (C)
3. `latencyMs` legal min/max? (C)
4. Keepalive interval on both WebSocket hosts? (D)
5. Subscribe-ack SLA + read-deadline ms? (D)
6. `status`/`appResponse` full enum + per-endpoint HTTP map? (F)
7. Order semantics — `success`+empty-`orderNo` possible? `GTC`/`INDEX`
   server-side? Money precision paise vs decimal? (F/E)
