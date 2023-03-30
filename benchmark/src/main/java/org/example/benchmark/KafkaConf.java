package org.example.benchmark;

import java.util.List;

public class KafkaConf {
    String topic;
    Integer topicNum;
    Long messageNum;
    Integer messageSize;
    List<String> producerProps;
    String producerConfig;
    String payloadFilePath;
    String transactionalId;
    Boolean shouldPrIntegerMetrics;
    Long transactionDurationMs ;
    Integer threadNum;
    Boolean asyncEnable;
    String bootstrapServer;
}
