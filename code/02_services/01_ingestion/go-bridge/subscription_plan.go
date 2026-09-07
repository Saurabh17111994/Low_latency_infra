package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

const (
	MaxHFTTokensPerConnection = 1024
	MaxHFTTokensPerRequest    = 512
	MaxHFTConnections         = 3
)

type SlotAssignment struct {
	SlotID       string
	ConnectionID string
	Tokens       []int32
	Requests     [][]int32
}

type SubscriptionPlan struct {
	Slots       []SlotAssignment
	Fingerprint string
}

func BuildSubscriptionPlan(tokens []int32, slots, connectionLimit, requestLimit int) (SubscriptionPlan, error) {
	if len(tokens) == 0 {
		return SubscriptionPlan{}, fmt.Errorf("instrument token input must not be empty")
	}
	if slots <= 0 || slots > MaxHFTConnections {
		return SubscriptionPlan{}, fmt.Errorf("slot count must be between 1 and %d", MaxHFTConnections)
	}
	if connectionLimit <= 0 || connectionLimit > MaxHFTTokensPerConnection {
		return SubscriptionPlan{}, fmt.Errorf("connection limit must be between 1 and %d", MaxHFTTokensPerConnection)
	}
	if requestLimit <= 0 || requestLimit > MaxHFTTokensPerRequest {
		return SubscriptionPlan{}, fmt.Errorf("request limit must be between 1 and %d", MaxHFTTokensPerRequest)
	}
	if len(tokens) > slots*connectionLimit {
		return SubscriptionPlan{}, fmt.Errorf("token count %d exceeds capacity %d", len(tokens), slots*connectionLimit)
	}
	ordered := append([]int32(nil), tokens...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for i := 1; i < len(ordered); i++ {
		if ordered[i] == ordered[i-1] {
			return SubscriptionPlan{}, fmt.Errorf("duplicate token %d", ordered[i])
		}
	}
	// R-188: validate the token value domain — a zero or negative int32 token
	// would otherwise pass plan construction and fail only when the real
	// SubscribeHFTTokens request reached the broker.
	for _, t := range ordered {
		if t <= 0 {
			return SubscriptionPlan{}, fmt.Errorf("token %d is invalid (must be positive)", t)
		}
	}
	plan := SubscriptionPlan{}
	for i := 0; i < slots; i++ {
		start := i * connectionLimit
		if start >= len(ordered) {
			break
		}
		end := start + connectionLimit
		if end > len(ordered) {
			end = len(ordered)
		}
		assignment := SlotAssignment{SlotID: fmt.Sprintf("hft-%d", i), ConnectionID: fmt.Sprintf("hft-%d", i), Tokens: append([]int32(nil), ordered[start:end]...)}
		for j := 0; j < len(assignment.Tokens); j += requestLimit {
			k := j + requestLimit
			if k > len(assignment.Tokens) {
				k = len(assignment.Tokens)
			}
			assignment.Requests = append(assignment.Requests, append([]int32(nil), assignment.Tokens[j:k]...))
		}
		plan.Slots = append(plan.Slots, assignment)
	}
	h := sha256.New()
	// R-098: the fingerprint must cover the actual subscription topology — the
	// per-slot Requests partitioning and the requestLimit parameter — not just
	// SlotID/ConnectionID/Tokens. Two plans with identical token sets but
	// different chunking now hash differently.
	fmt.Fprintf(h, "requestLimit=%d|", requestLimit)
	for _, slot := range plan.Slots {
		fmt.Fprintf(h, "%s|%s|", slot.SlotID, slot.ConnectionID)
		for _, token := range slot.Tokens {
			fmt.Fprintf(h, "%d,", token)
		}
		fmt.Fprintf(h, "requests=")
		for _, req := range slot.Requests {
			fmt.Fprintf(h, "[")
			for _, t := range req {
				fmt.Fprintf(h, "%d,", t)
			}
			fmt.Fprintf(h, "]")
		}
	}
	plan.Fingerprint = hex.EncodeToString(h.Sum(nil))
	return plan, nil
}

// tokenSetHash returns the lowercase SHA-256 hex digest of a token set:
// tokens sorted ascending, each encoded as 8 big-endian bytes. This is
// byte-identical to the Java side (SafetyHaltWriter.computeAssignedTokenHash
// and InstrumentManifestLoader.computeFingerprint), so the BridgeEvent
// manifest_fingerprint / assigned_token_set_hash fields line up across the
// bridge. The sort+hash happens here so both fields share one implementation.
func tokenSetHash(tokens []int32) string {
	ordered := append([]int32(nil), tokens...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	h := sha256.New()
	var buf [8]byte
	for _, t := range ordered {
		binary.BigEndian.PutUint64(buf[:], uint64(int64(t)))
		h.Write(buf[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SlotForToken maps a token to its owning slot index, or -1 when the token is
// not part of the plan. The mapping is deterministic and consistent with
// BuildSubscriptionPlan's sharding (T1): tokens are sorted ascending and
// carved into slices of connectionLimit per slot — the 3000-token
// manifest with 3 slots yields 1024+1024+952. Slices of a sparse sorted list
// may span gap values that belong to no slot (hence exact membership, not
// bounds). Broker consumers deriving
// token→slot identity (per-slot scoping, discontinuity attribution) must use
// this same mapping rather than recomputing a modulo split, which would not
// match the plan's contiguous ranges.
func SlotForToken(plan SubscriptionPlan, token int32) int {
	for i := range plan.Slots {
		tokens := plan.Slots[i].Tokens
		// P1-029: exact membership, not bounds. Ranges are sorted but NOT
		// gap-free for sparse inputs (builder sorts/dedups, never
		// densifies), so a bounds hit on a gap value is a false positive.
		// Tokens per slot are sorted ascending (see BuildSubscriptionPlan).
		j := sort.Search(len(tokens), func(k int) bool { return tokens[k] >= token })
		if j < len(tokens) && tokens[j] == token {
			return i
		}
	}
	return -1
}
