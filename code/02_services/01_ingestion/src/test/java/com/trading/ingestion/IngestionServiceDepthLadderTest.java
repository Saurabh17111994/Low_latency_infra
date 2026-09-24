package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The v4 depth ladders are zero-filled, never index-checked.
 *
 * <p>A tick whose book is empty, or shorter than the five stored levels, must
 * still append: the missing levels are 0. Regression lock for the
 * IndexOutOfBoundsException (Index:0, Size:0) that used to fail the whole tick
 * as a processing error -- which the failure matrix saw as a silently dropped
 * tick, not as a bad book.
 */
final class IngestionServiceDepthLadderTest {

    @Test
    @DisplayName("an empty ladder becomes five zeros without reading a level")
    void emptyLadderIsZeroFilled() {
        assertArrayEquals(new long[] {0, 0, 0, 0, 0},
                IngestionService.depthLadder(0, i -> {
                    throw new AssertionError("must not read level " + i + " of an empty ladder");
                }));
    }

    @Test
    @DisplayName("a short ladder keeps its levels and zero-fills the rest")
    void shortLadderIsZeroFilled() {
        assertArrayEquals(new long[] {11, 22, 0, 0, 0},
                IngestionService.depthLadder(2, i -> 11L * (i + 1)));
    }

    @Test
    @DisplayName("a full ladder is copied in order")
    void fullLadderIsCopied() {
        assertArrayEquals(new long[] {100, 200, 300, 400, 500},
                IngestionService.depthLadder(5, i -> 100L * (i + 1)));
    }

    @Test
    @DisplayName("a longer ladder is truncated to the five stored levels")
    void longLadderIsTruncated() {
        long[] out = IngestionService.depthLadder(9, i -> i + 1);
        assertEquals(5, out.length);
        assertArrayEquals(new long[] {1, 2, 3, 4, 5}, out);
    }
}
