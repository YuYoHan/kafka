package org.example.kafka.config;

import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
public class HdfSinkApplication {
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3;
}
