module github.com/trading/execution-arrow-bridge

go 1.24.5

require (
	github.com/arrow-trade/go-arrow v0.2.0
	github.com/gorilla/websocket v1.5.3
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/boombuler/barcode v1.1.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pquerna/otp v1.5.0 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.65.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

// Use the exact audited SDK tree already consumed by the market-data bridge.
// The execution bridge is a separate module and does not modify that bridge
// or the upstream SDK.
replace github.com/arrow-trade/go-arrow => ../../01_ingestion/go-bridge/third_party/go-arrow
