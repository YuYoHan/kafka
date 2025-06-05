package com.example.practice.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer <K extends Serializable, V extends Serializable> {
    public static final Logger log = LoggerFactory.getLogger(BaseConsumer.class.getName());
    private KafkaConsumer<K, V> consumer;
    private List<String> topics;

    public BaseConsumer(Properties consumerProps, List<String> topics) {
        this.consumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }


    public void initConsumer() {
        this.consumer.subscribe(topics);
        shutDownHookToRuntime(this.consumer);
    }

    private void shutDownHookToRuntime(KafkaConsumer<K,V> consumer) {
        // main thread
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thrad로 consumer wakeup()메소드를 호출하게 함
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.debug("main program starts to exit by calling wakeup");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void processRecord(ConsumerRecord<K, V> record) {
        log.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<K, V> records) {
        records.forEach(record -> processRecord(record));
    }

    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        }catch(WakeupException e) {
            log.error("wakeup exception has been called");
        }catch(Exception e) {
            log.error(e.getMessage());
        }finally {
            log.info("##### commit sync before closing");
            consumer.commitSync();
            log.info("finally consumer is closing");
            closeConsumer();
        }
    }

    private void pollCommitAsync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.consumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        this.consumer.commitAsync((offset, exception) -> {
            if(exception != null) {
                log.error("offsets {} is not completed, error:{}", offset, exception.getMessage());
            }
        });
    }

    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.consumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if(consumerRecords.count() > 0) {
                this.consumer.commitSync();
                log.debug("commit sync has been called");
            }
        } catch (CommitFailedException e) {
            log.error(e.getMessage());
        }
    }
    public void closeConsumer() {
        this.consumer.close();
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<String, String>(props, List.of(topicName));
        baseConsumer.initConsumer();
        String commitMode = "async";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();
    }
}
