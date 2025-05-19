package com.example.producers.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerSync.class.getName());
    public static void main(String[] args) {
        // 메시지 보낼 TOPIC 이름 설정
        String topicName = "simple-topic";

        // KafkaProducer 환경 설정
        Properties props = new Properties();
        // bootstrap.servers, Producer -> Serialization -> Broker -> Consumer -> Deserialization
        // key.serializer.class, value.serializer.class
        // null, "hello world"
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<Object, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<Object, String> producerRecord = new ProducerRecord<>(topicName, "hello kafka2");

        try {
            // KafkaProducer message send
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            LOGGER.info("record metadata received \n" +
                    "partition : " + recordMetadata.partition() + "\n" +
                    "offset : " + recordMetadata.offset() + "\n" +
                    "timestamp : " + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }

        // 메시지 flush 및 close
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
