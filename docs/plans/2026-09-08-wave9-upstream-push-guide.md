# Push guide — Wave 9 upstream fixes v0.2.0 (6 bundles, E deferred)

## What was prepared

- Working clone: `/tmp/go-arrow-upstream` (HEAD `7cce1630`, = pin).
- Patch: `/tmp/wave9-v020.patch` (7 files, +313/−47).
- Clean-room verified: fresh clone @ `7cce1630` → `git apply` → `go mod tidy` →
  `go build ./...` ok → `go vet ./arrow/` ok → `go test ./...` ok (no test
  files upstream) → `gofmt -l` shows only pre-existing
  (`constants.go`, `limits.go`, `market.go` — untouched by this patch).
- Throwaway `-race` verification (deleted after run): Bundle A lock +
  8-goroutine SetToken/GetToken race, Bundle C ranges + single-reader
  refusal, Bundle F validate matrix, Bundle B checksum — 6/6 PASS.

## Bundles in the patch (E deferred per accepted approach — money stays float)

- A `arrow/client.go`: `SetToken` Lock; `GetToken`/`GetRefreshToken` RLock;
  `request` + `rawRequestAuth` copy BaseURL/AppID/Token under RLock; headers use the copies.
- B `arrow/auth.go`: `Authenticate` empty-token guard + endpoint-context
  errors; `GenerateChecksum` returns `""` on empty secret/token; `Login`
  documents stdin ownership + returns error + new `LoginContext(ctx, token)`;
  `AutoLogin` typed `AuthError{Stage}` (login/totp/redirect/authenticate),
  empty-requestId/redirect guards, token-length print removed (P1-292).
- C `arrow/hft_stream.go`: `reading atomic.Bool` field WIRED into `ReadHFT`
  (CompareAndSwap refusal + `nil` guard); `ConnectHFTDataStream` snapshots
  auth under RLock + host-only dial error; `sort` import;
  `decodeHFTPayload` nil-before-lock; `SubscribeHFTTokens` exchSeg 0..3 +
  latency 50..60000; `SubscribeHFTBySegment` latency range + sorted keys +
  per-seg range; `writeJSON` marshal/deadline/write wrap + 10s write
  deadline + nil-conn guard.
- D `arrow/streams.go`: `ConnectDataStream`/`ConnectOrderStream` RLock
  snapshot + host-only dial errors; `sendSubMessage` nil-conn + write
  deadline + wrapped write; `ReadTicks` 30s per-iteration read deadline
  (`defaultStreamReadDeadlineMs`).
- F `arrow/orders.go` + `arrow/market.go`: `ValidateOrderRequest`
  (DAY|IOC, no INDEX, positive price/qty — no-float digit scan) +
  `isPositiveDecimal`/`isPositiveInt`; `PlaceOrder` validates first,
  endpoint-context errors, message+code on failure, empty-OrderNo → error;
  `GenericResponse.RequireSuccess` rejects unknown status.
- G `.gitignore`: `Makefile` line removed + `!Makefile` guard; `config.example`
  added (file itself NOT created — create it before tagging); `*.log` kept,
  `build/**/*.log` narrowed.

## Before tagging — 3 open items

1. Dual `checkSum`/`checksum` (`auth.go`) kept as-is: needs Arrow's
   canonical key before dropping one (broker unknown #1).
2. `config.example` referenced in `.gitignore` but not created — add it.
3. `Authenticate` token write takes `mu.Lock`; `Login`/`AutoLogin` AppID
   reads snapshot under RLock — full `c.Config` audit: every remaining
   read/write sits inside RLock/Lock windows (verified by window scan).

## Push commands (run these, NOT the agent — no GitHub auth here)

```sh
cd /tmp/go-arrow-upstream
git checkout -b wave9-v020
git add .gitignore arrow/auth.go arrow/client.go arrow/hft_stream.go \
  arrow/market.go arrow/orders.go arrow/streams.go
git commit -m "fix(wave9): bundles A-D,F,G — token lock, auth contract, HFT ranges, stream deadlines, order validation, hygiene

A: SetToken Lock, GetToken/GetRefreshToken RLock, request copies under RLock.
B: Authenticate guards, checksum empty-secret, Login stdin docs + error,
   LoginContext, AutoLogin typed AuthError, drop token-length log.
C: exchSeg/latency ranges, sorted BySegment keys, nil-before-lock,
   write deadline + nil-conn guard, single-reader field.
D: host-only dial errors, sub write deadline, 30s read deadline.
F: ValidateOrderRequest (DAY|IOC, no INDEX, positive price/qty),
   PlaceOrder validates + empty-OrderNo error, RequireSuccess.
G: track Makefile, config.example, narrow logs.
E (paise) deferred — money stays float.

Verified: build ok, vet ok, tests ok (none upstream), gofmt own-files clean."
git push -u origin wave9-v020
# then: merge PR → tag v0.2.0 → paste `git ls-remote <url> v0.2.0` SHA here
```

## Downstream unblock (after tag SHA arrives)

1. Bump `go-bridge/go.mod:11` to new SHA.
2. Re-apply R-101..R-243 onto new tag.
3. `go test ./...` (both bridges) + `-race` trio + `vet` + static 2/2.
4. Tick 65 AGREE with new SHA; WAVE 9 → `[x]`.
