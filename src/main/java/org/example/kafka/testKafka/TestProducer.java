package org.example.kafka.testKafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TestProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send() {
        kafkaTemplate.send("mytopic", "spring kafkaTest...");
    }
}
