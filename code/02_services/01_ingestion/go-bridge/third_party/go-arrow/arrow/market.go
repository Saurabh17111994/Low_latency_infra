package arrow

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type GenericResponse[T any] struct {
	Data   T      `json:"data"`
	Status string `json:"status"`
}

// RequireSuccess rejects unknown status values instead of zero-value
// accepting them: only "success" passes; "error" maps to errWithStatus
// and anything else is an explicit unknown-status error so a new broker
// enum can never slip through as success. (WAVE9-F, P1-041/042.)
func (r GenericResponse[T]) RequireSuccess(op string) error {
	switch r.Status {
	case "success":
		return nil
	case "error", "":
		return fmt.Errorf("%s failed with status: %s", op, r.Status)
	default:
		return fmt.Errorf("%s failed: unknown status %q", op, r.Status)
	}
}

type BasketMarginRequest struct {
	Orders           []MarginRequest `json:"orders"`
	IncludePositions bool            `json:"includePositions"`
}

func (c *Client) GetBasketMargin(req BasketMarginRequest) (map[string]any, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.request("/margin/basket", "POST", payload)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[map[string]any]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface the server's errorMessage/errorCode, not a bare status.
	if result.Status != "success" {
		return nil, apiError("basket margin", result.Status, resp)
	}
	if result.Data == nil {
		return map[string]any{}, nil
	}
	return result.Data, nil
}

// GetGreeks posts an array of instrument tokens to /info/greeks.
// The server may return 400 when Greeks are unavailable for the given tokens.
func (c *Client) GetGreeks(tokens []int) (json.RawMessage, error) {
	payload, err := json.Marshal(tokens)
	if err != nil {
		return nil, err
	}
	resp, err := c.request("/info/greeks", "POST", payload)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[json.RawMessage]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface errorMessage/errorCode.
	if result.Status != "success" {
		return nil, apiError("greeks", result.Status, resp)
	}
	return result.Data, nil
}

type OptionChainRequest struct {
	Underlying string   `json:"underlying"`
	Exchange   Exchange `json:"exchange"`
	Count      string   `json:"count"`
	Expiry     string   `json:"expiry"`
}

func (c *Client) GetOptionChain(req OptionChainRequest) (json.RawMessage, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.request("/info/option-chain", "POST", payload)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[json.RawMessage]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface errorMessage/errorCode.
	if result.Status != "success" {
		return nil, apiError("option chain", result.Status, resp)
	}
	return result.Data, nil
}

// OptionChainSymbolsByCategory is the object under response "data" for option-chain symbol listings.
// Keys are categories (e.g. "equity", "indices"); values map listing ids such as "NSE:RELIANCE-EQ"
// or "INDEX:NIFTY" to available expiry date strings.
type OptionChainSymbolsByCategory map[string]map[string][]string

// GetAllOptionChainSymbols fetches all option-chain symbol listings and expiries (GET /info/option-chain-symbols/all).
func (c *Client) GetAllOptionChainSymbols() (OptionChainSymbolsByCategory, error) {
	resp, err := c.request("/info/option-chain-symbols/all", "GET", nil)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[OptionChainSymbolsByCategory]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface errorMessage/errorCode.
	if result.Status != "success" {
		return nil, apiError("option chain symbols", result.Status, resp)
	}
	if result.Data == nil {
		return OptionChainSymbolsByCategory{}, nil
	}
	return result.Data, nil
}

// HolidaysData is the object under "data" for GET /info/holidays.
type HolidaysData struct {
	Holidays           map[string]string          `json:"holidays"`
	SpecialTradingDays map[string]json.RawMessage `json:"specialTradingDays"`
}

func (c *Client) GetHolidays() (*HolidaysData, error) {
	resp, err := c.request("/info/holidays", "GET", nil)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[HolidaysData]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface errorMessage/errorCode.
	if result.Status != "success" {
		return nil, apiError("holidays", result.Status, resp)
	}
	return &result.Data, nil
}

func (c *Client) GetIndexList() ([]map[string]any, error) {
	resp, err := c.request("/info/index-list", "GET", nil)
	if err != nil {
		return nil, err
	}
	var result GenericResponse[[]map[string]any]
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	// R-282: surface errorMessage/errorCode.
	if result.Status != "success" {
		return nil, apiError("index list", result.Status, resp)
	}
	return result.Data, nil
}

type InstrumentSegment string

const (
	InstrumentSegmentAll     InstrumentSegment = "all"
	InstrumentSegmentNSE     InstrumentSegment = "nse"
	InstrumentSegmentBSE     InstrumentSegment = "bse"
	InstrumentSegmentMCX     InstrumentSegment = "mcx"
	InstrumentSegmentIndices InstrumentSegment = "indices"
)

func (c *Client) GetInstrumentsCSV(segment InstrumentSegment) (string, error) {
	// R-189: path-escape the segment — interpolating raw input into the URL
	// path could inject unexpected segments.
	seg := strings.ToLower(string(segment))
	if seg == "" {
		seg = "all"
	}
	path := "/" + url.PathEscape(seg)
	resp, err := c.request(path, "GET", nil)
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func (c *Client) GetInstruments(segment InstrumentSegment) ([][]string, error) {
	csvText, err := c.GetInstrumentsCSV(segment)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(strings.NewReader(csvText))
	return r.ReadAll()
}

// GetCandleData calls the Historical Data API (GET /candle/:exchange/:token/:interval).
// Query parameters from and to must be in yyyy-MM-ddTHH:mm:ss form; oi adds oi=1 (NFO only, extra OHLC field in each row).
// On success the body is a JSON array of candle rows (arrays), as in the docs — not the usual REST {data,status} envelope.
// On failure the HTTP API may return 4xx JSON such as {"status":"error","errorMessage":"invalid token","errorCode":"BadRequestError"}:
// that means the exchange/token pair is not recognised (wrong segment or expired derivatives token), not a client-side parse error.
// See https://docs.arrow.trade/rest-api/historical-candle-data/
func (c *Client) GetCandleData(exchange Exchange, token, interval, fromTimestamp, toTimestamp string, oi bool) (json.RawMessage, error) {
	// R-104: honor the configurable historical base URL like every other
	// method honors BaseURL — the endpoint was previously hardcoded to the
	// production host, bypassing config entirely.
	base := c.Config.HistoricalBaseURL
	q := url.Values{}
	q.Set("from", fromTimestamp)
	q.Set("to", toTimestamp)
	if oi {
		q.Set("oi", "1")
	}
	// R-189: path-escape exchange/token/interval.
	endpoint := fmt.Sprintf("%s/candle/%s/%s/%s?%s", base,
		url.PathEscape(strings.ToLower(string(exchange))),
		url.PathEscape(token), url.PathEscape(interval), q.Encode())
	resp, err := c.rawRequestAuth(endpoint, "GET", nil)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimSpace(resp)), nil
}

// apiError builds a descriptive error for a non-success API response (R-282):
// it parses the server's errorMessage/errorCode fields so operators see the
// real rejection reason instead of a bare status string.
func apiError(op, status string, resp []byte) error {
	var envelope struct {
		ErrorMessage string `json:"errorMessage"`
		ErrorCode    string `json:"errorCode"`
		Message      string `json:"message"`
	}
	_ = json.Unmarshal(resp, &envelope) // best-effort; keep the status on failure
	msg := envelope.ErrorMessage
	if msg == "" {
		msg = envelope.Message
	}
	if msg == "" {
		return fmt.Errorf("%s failed with status: %s", op, status)
	}
	if envelope.ErrorCode != "" {
		return fmt.Errorf("%s failed with status: %s, code: %s, message: %s",
			op, status, envelope.ErrorCode, msg)
	}
	return fmt.Errorf("%s failed with status: %s, message: %s", op, status, msg)
}
