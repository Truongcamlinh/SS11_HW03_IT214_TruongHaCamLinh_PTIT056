package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryOrderConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryOrderConsumer.class);

    @KafkaListener(topics = "${storex.kafka.order-topic}")
    public void consume(ConsumerRecord<String, OrderCreatedEvent> record) {
        log.info("Tru kho don {} tai instance={}, partition={}, offset={}",
                record.value().orderId(), System.getenv().getOrDefault("INSTANCE_ID", "inventory-1"),
                record.partition(), record.offset());
    }
}

