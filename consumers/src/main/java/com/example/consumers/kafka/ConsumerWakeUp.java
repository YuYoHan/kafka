package com.example.consumers.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeUp {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWakeUp.class.getName());
    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(topicName));

        // main thread
        Thread mainThread = Thread.currentThread();


        // main thread 끝나기전에 마지막으로 동작을 시킬 수 있도록 하는 메소드
        // main thread 종료시 별도의 thread로 kafkaConsumer wakeup 실행
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.debug("main program starts to exit calling wakeup() method");
                // kafkaConumser.poll() 중에 exception을 발생시키는 용도
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while(true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord record : consumerRecords) {
                    LOGGER.debug("record key : {}, record value : {}, partition : {}",
                            record.key(), record.value(), record.partition());
                }
            }
        } catch (WakeupException e) {
            LOGGER.error("wakeup exception has been called");
        } finally {
            LOGGER.debug("finally consumer is closing");
            kafkaConsumer.close();
        }

    }
}
