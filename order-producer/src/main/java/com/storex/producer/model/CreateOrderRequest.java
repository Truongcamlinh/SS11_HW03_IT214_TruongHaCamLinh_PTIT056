package com.storex.producer.model;

import java.math.BigDecimal;

public record CreateOrderRequest(String orderId, String customerId, BigDecimal totalAmount) {
}

