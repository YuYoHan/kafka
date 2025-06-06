package com.example.practice.consumer;

import com.example.practice.model.OrderModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<OrderModel> {
    private static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(String topic, byte[] data) {
        if (data == null) {
            logger.warn("Received null data for topic: {}", topic);
            return null;
        }

        try {
            return objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            logger.error("Failed to deserialize message on topic {}: {}", topic, e.getMessage());
            return null;
        }
    }
}
