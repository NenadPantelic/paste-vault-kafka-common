package com.pastevault.kafka_common.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "kafka.config")
public record KafkaConfigProperties(String bootstrapServers,
                                    String schemaRegistryUrlKey,
                                    String schemaRegistryUrl,
                                    String topicName,
                                    List<String> topicNamesToCreate,
                                    Integer numOfPartitions,
                                    Short replicationFactor) {
}