package com.storex.producer.model;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String eventType,
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        Instant createdAt) {

    public static OrderCreatedEvent create(String orderId, String customerId, BigDecimal totalAmount) {
        return new OrderCreatedEvent("order.created", orderId, customerId, totalAmount, Instant.now());
    }
}

