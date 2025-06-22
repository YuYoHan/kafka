package com.example.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStream {
    private static String APPLICATION_NAME = "KafkaStreamApplication";
    private static String BOOTSTRAP_SERVERS = "192.168.56.101:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "stream_log_copy";
    public static void main(String[] args) {

        Properties props = new Properties();
        // 스트림즈 애플리케이션은 애플리케이션 아이디(application.id)를 지정해야 한다.
        // 애플리케이션 아이디 값을 기준으로 병렬처리하기 때문이다.
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        // 스트림즈 애플리케이션과 연동할 카프카 클러스터 정보를 입력한다. 컨슈머나 프로듀서 처럼
        // 1개 이상의 카프카 브로커 호스트와 포트 정보를 입력한다.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // StreamsBuilder는 스트림 토폴로지를 정의하기 위한 용도로 사용된다.
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        streamLog.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
