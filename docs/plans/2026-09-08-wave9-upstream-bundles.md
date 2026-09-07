# Wave 9 upstream fix bundles (vendored `third_party/go-arrow`, pin-only)

65 AGREE items grouped by vendored file. Each bundle = one upstream change.
Apply when the vendored tree opens; NEVER hand-edit in place (re-pin instead).
Proof: `-race` repro `go-bridge/vendored_race_repro_test.go`
(`PROVE_VENDORED_RACE=1 go test -race -run TestVendoredTokenRaceRepro .`
fires on `third_party/go-arrow/arrow/client.go:198` vs `:233`,
captured `/tmp/w9race.txt` 2026-09-08).

## Bundle A — `arrow/client.go` (P1-031, +DUPs 032/033, P1-035)

- Add `sync.RWMutex` to `Client`; `SetToken`/`GetToken`/`GetRefreshToken`/
  `SetDebug`/`IsDebug` + every `c.Config.*` read in `request`/`requestWith`
  take it. Fixes token race + narrows 032/033 to the same lock.
- `request`/`requestWith`: copy `BaseURL/AppID/Token` under RLock before
  building the fasthttp request (P1-035 read side).

## Bundle B — `arrow/auth.go` (P1-161,163,164,165,166,167,292,293,294)

- `Authenticate`: validate non-empty requestToken (P1-161); wrap
  marshal/do/unmarshal errors with endpoint context (P1-166); drop the
  dual `CheckSum/Checksum` key after broker confirms canonical (P1-293).
- `Login`: document stdin ownership + add timeout or context variant (P1-162
  MITIGATED on our path — AutoLogin-only, zero `.Login(` callers).
- `AutoLogin`: return typed `AuthError` (distinguish bad-creds vs bad-TOTP vs
  network, P1-163/164); stop printing token-length side channel (P1-292);
  `GenerateChecksum`: reject empty secret (P1-294).

## Bundle C — `arrow/hft_stream.go` (P1-037,038,039,176,178)

- Single-reader guard: `atomic.Bool` in `ReadHFTWithFrame`, second caller gets
  an error (P1-037; our path already single — `main.go:406` one goroutine).
- Nil-receiver guard on `Close`/`decodeHFTPayload` (P1-038; ours never nil —
  factory never returns nil stream with nil err).
- Length-gate before `hftPacketMeta` dispatch (P1-039).
- `normalizeHFTMode` error mentions valid set (P1-176); `writeJSON` deadline
  error wrapped with op context (P1-178).

## Bundle D — `arrow/streams.go` (P1-045,046,047,199,200,202,302)

- `connectDataStreamURL`/`connectOrderStreamURL`: handshake error wrapped with
  URL context minus secrets (P1-045/046); keepalive documented or configurable
  (P1-047); nil-conn guard (P1-199); `ReadMessage` loop needs the same
  read-deadline treatment hft got via R-103 (P1-200, +DUP 201); subscribe-ack
  timeout instead of indefinite wait (P1-202); `OrderUpdate` doc field names
  (P1-302).

## Bundle E — numeric types (P1-040 margin, P1-044 positions, P1-196/197/198 quote, P1-299)

- `margin.go`, `positions.go:string`, `quote.go`: float64/string money →
  integer paise or documented decimal with overflow checks. Our pipeline
  already converts at the boundary; upstream type change is breaking — major
  bump with migration note.

## Bundle F — validation at parse (P1-041/042 market, P1-185..188, P1-298, P1-189..193 orders, P1-194/195 positions, P1-179 holdings, P1-180/181 limits, P1-182..184 margin, P1-203..205 user, P1-295/296 client triplication, P1-297 doc, P1-300/301 quote)

- Envelope: reject unknown `appResponse` values instead of zero-value accept
  (P1-041/042); per-endpoint HTTP status mapping (P1-185..188); doc.go correct
  import path + TOTP-only auth (P1-297).
- `orders.go`: `OrderNo` empty check (P1-043 MITIGATED — exec `broker.go`
  rejects), `Validity`/`Exchange` enums reject GTC/INDEX (P1-036/172
  MITIGATED at exec layer), price/qty > 0 (P1-191).
- `client.go` triplication: one `doRequest` helper (P1-295/296 DUP pair).

## Bundle G — `.gitignore`/repo hygiene (P1-030,159,160)

- Track `Makefile` deliberately or document why ignored (P1-030);
  `.env` never committed + `config.example` (P1-159); narrow `*.log` to
  build dirs (P1-160).

## Disposition of the other 14

- MITIGATED (5): P1-036/043/162/172/177 — flaw real, our path neutralizes;
  no urgent push, bundle references our guard.
- DUP (9): P1-032/033/170/171/201/206/207/208/295 — fixed by parent bundle.
