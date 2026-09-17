package com.storex.loyalty.consumer;

import com.storex.loyalty.model.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LoyaltyOrderConsumer {
    private static final Logger log = LoggerFactory.getLogger(LoyaltyOrderConsumer.class);

    @KafkaListener(topics = "${storex.kafka.order-topic}")
    public void consume(ConsumerRecord<String, OrderCreatedEvent> record) {
        long points = record.value().totalAmount().longValue() / 10_000;
        log.info("Cong {} diem cho customer={} tu don={}, partition={}, offset={}",
                points, record.value().customerId(), record.value().orderId(),
                record.partition(), record.offset());
    }
}

