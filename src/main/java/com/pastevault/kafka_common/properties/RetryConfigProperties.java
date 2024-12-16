package com.pastevault.kafka_common.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "retry")
public record RetryConfigProperties(Long initialIntervalMs,
                                    Long maxIntervalMs,
                                    Double multiplier,
                                    Integer maxAttempts,
                                    Long sleepTimeMs) {


}