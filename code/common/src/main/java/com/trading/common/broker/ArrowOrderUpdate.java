package com.trading.common.broker;

import com.trading.common.arrow.ArrowOrderStatus;
import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import java.util.Objects;

/**
 * Arrow postback event from {@code wss://order-updates.arrow.trade}.
 *
 * This is the Action Capture source for the {@code Fills} table. Identity mapping:
 * {@code id} -> {@link BrokerOrderId}, {@code remarks} -> {@link ClientOrderRef}
 * (the platform round-trip ref, max 16 chars), {@code token} -> {@link InstrumentToken}.
 *
 * <p>R-172: {@code fillTime} is epoch <b>milliseconds</b>, matching
 * {@link com.trading.common.arrow.ArrowOrderResponse#requestTime} and the Fills
 * DDL {@code broker_event_time BIGINT} (which carries no unit comment — this is
 * the canonical unit for broker timestamps in this module).
 */
public final class ArrowOrderUpdate {

    private final BrokerOrderId brokerOrderId;
    private final ClientOrderRef clientOrderRef;
    private final InstrumentToken instrumentToken;
    private final ArrowOrderStatus.OrderStatus status;
    private final ArrowOrderStatus.ReportType reportType;
    private final String fillId; // trade-book fillID when reportType == FILL
    private final long fillQuantity;
    private final long fillPrice; // paise
    private final long fillTime; // epoch ms (R-172)
    private final String exchangeOrderId;
    private final String rejectReason;

    private ArrowOrderUpdate(Builder b) {
        this.brokerOrderId = b.brokerOrderId;
        this.clientOrderRef = b.clientOrderRef;
        this.instrumentToken = b.instrumentToken;
        this.status = b.status;
        this.reportType = b.reportType;
        this.fillId = b.fillId;
        this.fillQuantity = b.fillQuantity;
        this.fillPrice = b.fillPrice;
        this.fillTime = b.fillTime;
        this.exchangeOrderId = b.exchangeOrderId;
        this.rejectReason = b.rejectReason;
    }

    // R-262: named-parameter construction (builder) instead of the 11-parameter
    // positional constructor whose three adjacent longs made quantity/price/
    // time swaps compile silently.

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private BrokerOrderId brokerOrderId;
        private ClientOrderRef clientOrderRef;
        private InstrumentToken instrumentToken;
        private ArrowOrderStatus.OrderStatus status;
        private ArrowOrderStatus.ReportType reportType;
        private String fillId;
        private long fillQuantity;
        private long fillPrice;
        private long fillTime;
        private String exchangeOrderId;
        private String rejectReason;

        public Builder brokerOrderId(BrokerOrderId v) { this.brokerOrderId = v; return this; }
        public Builder clientOrderRef(ClientOrderRef v) { this.clientOrderRef = v; return this; }
        public Builder instrumentToken(InstrumentToken v) { this.instrumentToken = v; return this; }
        public Builder status(ArrowOrderStatus.OrderStatus v) { this.status = v; return this; }
        public Builder reportType(ArrowOrderStatus.ReportType v) { this.reportType = v; return this; }
        public Builder fillId(String v) { this.fillId = v; return this; }
        public Builder fillQuantity(long v) { this.fillQuantity = v; return this; }
        public Builder fillPrice(long v) { this.fillPrice = v; return this; }
        public Builder fillTime(long v) { this.fillTime = v; return this; }
        public Builder exchangeOrderId(String v) { this.exchangeOrderId = v; return this; }
        public Builder rejectReason(String v) { this.rejectReason = v; return this; }

        public ArrowOrderUpdate build() {
            Objects.requireNonNull(brokerOrderId, "brokerOrderId");
            Objects.requireNonNull(instrumentToken, "instrumentToken");
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(reportType, "reportType");
            // P3-109: a FILL without its trade identity or amounts is not a Fill row.
            // Scoped to FILL on purpose: the other report types carry no fill payload.
            if (reportType == ArrowOrderStatus.ReportType.FILL) {
                if (fillId == null || fillId.isBlank()) {
                    throw new IllegalStateException("fillId required when reportType==FILL");
                }
                if (fillQuantity <= 0) {
                    throw new IllegalStateException("fillQuantity must be > 0 when reportType==FILL");
                }
                if (fillPrice <= 0) {
                    throw new IllegalStateException("fillPrice must be > 0 when reportType==FILL");
                }
                // P3-109 follow-up: fillTime is the Fills row's broker_event_time (see the
                // class javadoc and ddl/08_fills.sql), so a zero value writes 1970-01-01
                // into the event-time column of an otherwise complete fill row.
                if (fillTime <= 0) {
                    throw new IllegalStateException("fillTime must be > 0 when reportType==FILL");
                }
            }
            return new ArrowOrderUpdate(this);
        }
    }

    public BrokerOrderId brokerOrderId() { return brokerOrderId; }
    /**
     * P3-338: Arrow's {@code remarks} — the platform round-trip ref.
     *
     * <p>Absent when the broker omits remarks. That is not hypothetical: the Go
     * bridge reads the field with {@code stringField(update, "remarks")}
     * (postback.go:198), which yields an empty string instead of failing, so such
     * events do reach this model. Consumers must treat an absent ref as unjoinable
     * — never as a match — because the platform correlates fills back through it.
     */
    public /* @Nullable */ ClientOrderRef clientOrderRef() { return clientOrderRef; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public ArrowOrderStatus.OrderStatus status() { return status; }
    public ArrowOrderStatus.ReportType reportType() { return reportType; }
    public String fillId() { return fillId; }
    public long fillQuantity() { return fillQuantity; }
    public long fillPrice() { return fillPrice; }
    public long fillTime() { return fillTime; }
    public String exchangeOrderId() { return exchangeOrderId; }
    public String rejectReason() { return rejectReason; }

    public boolean isFill() { return reportType == ArrowOrderStatus.ReportType.FILL; }
}
