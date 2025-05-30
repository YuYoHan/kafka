package com.example.producers.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public static  final Logger LOGGER = LoggerFactory.getLogger(CustomPartitioner.class.getName());
    private String specialKeyName;


    // 여기서 가져오는 값은 Properties props = new Properties(); 여기에 담는 값을 가져온다.
    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("specialKey").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5);
        int partitionIndex = 0;

        if(keyBytes == null) {
            throw new InvalidRecordException("keyBytes is null");
        }

        if(((String)key).equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }
        LOGGER.info("topic : {}, key : {}, partitionIndex : {}", topic, key, partitionIndex);
        return partitionIndex;
    }

    @Override
    public void close() {

    }


}
