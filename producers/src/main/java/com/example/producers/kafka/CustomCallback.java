package com.example.producers.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {
    public static final Logger LOGGER = LoggerFactory.getLogger(CustomCallback.class.getName());
    private int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception == null) {
            LOGGER.info("seq : {}, partition : {}, offset : {}", this.seq, metadata.partition(), metadata.offset());
        } else {
            LOGGER.error("exception error from broker " + exception.getMessage());
        }
    }
}
