package com.example.practice.consumer;

import com.example.practice.model.OrderModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class OrderDeserializer implements Deserializer<OrderModel> {
    public static final Logger logger = LoggerFactory.getLogger(OrderDeserializer.class.getName());
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public OrderModel deserialize(String topic, byte[] data) {
        OrderModel orderModel = null;

        try {
            orderModel = objectMapper.readValue(data, OrderModel.class);
        } catch (IOException e) {
            logger.error("Object mapper deserialization error" + e.getMessage());
        }

        return orderModel;
    }
}
