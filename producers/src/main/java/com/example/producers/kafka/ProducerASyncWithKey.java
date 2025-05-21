package com.example.producers.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncWithKey {
    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerASyncWithKey.class.getName());
    public static void main(String[] args) {
        // 메시지 보낼 TOPIC 이름 설정
        String topicName = "multipart-topic";

        // KafkaProducer 환경 설정
        Properties props = new Properties();
        // bootstrap.servers, Producer -> Serialization -> Broker -> Consumer -> Deserialization
        // key.serializer.class, value.serializer.class
        // null, "hello world"
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        for(int seq = 0; seq < 20; seq++) {
            // ProducerRecord 객체 생성
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello kafka " + seq);

            // KafkaProducer message send
            // new Callback을 람다식으로 변형
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if(exception == null) {
                    LOGGER.info("record metadata received \n" +
                            "partition : " + metadata.partition() + "\n" +
                            "offset : " + metadata.offset() + "\n" +
                            "timestamp : " + metadata.timestamp());
                } else {
                    LOGGER.error("exception error from broker " + exception.getMessage());
                }
            });
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 메시지 flush 및 close
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
