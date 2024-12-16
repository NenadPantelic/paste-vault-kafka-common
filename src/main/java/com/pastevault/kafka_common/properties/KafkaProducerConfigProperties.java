package com.pastevault.kafka_common.properties;


import org.springframework.boot.context.properties.ConfigurationProperties;

// NOTE: as of now this is fine, but in this way producer and consumer configs are tied within the same module
// maybe some service/consuming module does not need both configurations
@ConfigurationProperties(prefix = "kafka.producer")
public record KafkaProducerConfigProperties(String keySerializerClass,
                                            String valueSerializerClass,
                                            String topic,
                                            Integer requestsTimeoutMs,
                                            Integer retryCount) {
}