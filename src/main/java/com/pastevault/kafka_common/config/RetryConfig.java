package com.pastevault.kafka_common.config;

import com.pastevault.kafka_common.properties.RetryConfigProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    private final RetryConfigProperties retryConfigProperties;

    public RetryConfig(RetryConfigProperties retryConfigProperties) {
        this.retryConfigProperties = retryConfigProperties;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        ExponentialBackOffPolicy exponentialBackOffPolicy = new ExponentialBackOffPolicy();
        exponentialBackOffPolicy.setInitialInterval(retryConfigProperties.initialIntervalMs());
        exponentialBackOffPolicy.setMultiplier(retryConfigProperties.multiplier());
        exponentialBackOffPolicy.setMaxInterval(retryConfigProperties.maxIntervalMs());
        retryTemplate.setBackOffPolicy(exponentialBackOffPolicy);

        SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        simpleRetryPolicy.setMaxAttempts(retryConfigProperties.maxAttempts());
        retryTemplate.setRetryPolicy(simpleRetryPolicy);

        return retryTemplate;
    }
}