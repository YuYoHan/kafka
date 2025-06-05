package com.example.practice.producer;

import com.example.practice.event.EventHandler;
import com.example.practice.event.FileEventHandler;
import com.example.practice.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {
    private static final Logger log = LoggerFactory.getLogger(FileAppendProducer.class);

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
        boolean sync = false;
        File file = new File("C:\\study_\\springBoot\\kafka\\practice\\src\\main\\resources\\pizza_append.txt");
        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(20000, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }
}
