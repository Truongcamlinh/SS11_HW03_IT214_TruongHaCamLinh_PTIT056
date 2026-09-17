package com.storex.inventory.model;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String eventType,
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        Instant createdAt) {
}

