package com.example.practice.producer;

import com.example.practice.model.OrderModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;


public class OrderSerializer implements Serializer<OrderModel> {

    ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());


    @Override
    public byte[] serialize(String topic, OrderModel data) {
        byte[] serializedOrder = null;
        try {
            mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return serializedOrder;
    }
}
