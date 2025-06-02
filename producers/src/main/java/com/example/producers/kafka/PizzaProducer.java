package com.example.producers.kafka;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static final Logger LOGGER = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long send = 2025;
        Random random = new Random(send);
        Faker faker = new Faker(new Locale("ko"), random); // or Locale.ENGLISH

        while (iterSeq != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    LOGGER.info("IntervalCount : " + intervalCount + " intervalMills : " + interIntervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    LOGGER.info(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    LOGGER.info("record metadata received \n" +
                            "partition : " + metadata.partition() + "\n" +
                            "offset : " + metadata.offset() + "\n" +
                            "timestamp : " + metadata.timestamp());
                } else {
                    LOGGER.error("exception error from broker " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                LOGGER.info("record metadata received \n" +
                        "partition : " + recordMetadata.partition() + "\n" +
                        "offset : " + recordMetadata.offset() + "\n" +
                        "timestamp : " + recordMetadata.timestamp());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        // 메시지 보낼 TOPIC 이름 설정
        String topicName = "pizza-topic";

        // KafkaProducer 환경 설정
        Properties props = new Properties();
        // bootstrap.servers, Producer -> Serialization -> Broker -> Consumer -> Deserialization
        // key.serializer.class, value.serializer.class
        // null, "hello world"
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // batch 세팅
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");


        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName,
                -1, 500, 0, 0, true);

        // 메시지 flush 및 close
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
