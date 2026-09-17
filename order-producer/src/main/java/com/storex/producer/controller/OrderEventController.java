package com.storex.producer.controller;

import com.storex.producer.model.CreateOrderRequest;
import com.storex.producer.model.OrderCreatedEvent;
import com.storex.producer.service.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderEventController {
    private final OrderEventProducer producer;

    public OrderEventController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<OrderCreatedEvent> create(@RequestBody CreateOrderRequest request) {
        OrderCreatedEvent event = OrderCreatedEvent.create(
                request.orderId(), request.customerId(), request.totalAmount());
        producer.publish(event);
        return ResponseEntity.accepted().body(event);
    }
}

