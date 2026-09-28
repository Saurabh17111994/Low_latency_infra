package com.trading.execution.gateway;

/**
 * M1-6: the stored {@code Order_Lifecycle} row for the event's composite key could not be read.
 * The lifecycle write is refused (never a blind upsert) and the caller answers 503, so the event
 * stays retryable instead of silently overwriting durable state.
 */
final class OrderLifecycleLookupUnavailableException extends Exception {

    OrderLifecycleLookupUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
