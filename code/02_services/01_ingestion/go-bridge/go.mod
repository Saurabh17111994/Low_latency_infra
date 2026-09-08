module github.com/trading/arrow-bridge

go 1.24.5

// go-arrow is pinned to upstream tag v0.2.0 (tag object 67bc0b0 ->
// merge commit 1390d40, https://github.com/Saurabh17111994/go-arrow) =
// base 7cce1630 (2026-06-22) + Wave9 bundles A-D,F,G (E deferred),
// with in-repo patches R-101..R-243 merged on top (see
// CODE_REVIEW_REMEDIATION.md). The vendored replace makes the build
// self-contained; the require version is the upstream identity label and
// never resolves through the module proxy.
require github.com/arrow-trade/go-arrow v0.2.0

require (
	github.com/gorilla/websocket v1.5.3
	github.com/klauspost/compress v1.18.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/boombuler/barcode v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pquerna/otp v1.5.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.65.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

// The Arrow HFT SDK is vendored in-repo (third_party/go-arrow) so the image
// build is self-contained and reproducible. Relative replace — resolves
// relative to this go.mod both on the host and inside the Docker build.
replace github.com/arrow-trade/go-arrow => ./third_party/go-arrow
