package com.example.practice.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {
    public static final Logger LOGGER = LoggerFactory.getLogger(FileProducer.class.getName());
    public static void main(String[] args) {
        // 메시지 보낼 TOPIC 이름 설정
        String topicName = "file-topic";

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
        String filePath = "C:\\study_\\springBoot\\kafka\\practice\\src\\main\\resources\\pizza_sample.txt";

        // KafkaProducer 객체 생성 -> ProducerRecords 생성 -> send() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);

        kafkaProducer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for(int i = 1; i < tokens.length; i++) {
                    if(i != (tokens.length - 1)) {
                        value.append(tokens[i] + delimiter);
                    } else {
                        value.append(tokens[i]);
                    }
                }
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {
        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        LOGGER.debug("key : {}, value : {}", key, value);

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
}
