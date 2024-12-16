package com.pastevault.kafka_common.config;

import com.pastevault.kafka_common.properties.KafkaConfigProperties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

import java.util.Map;

@Configuration
@EnableRetry
public class KafkaAdminConfig {

    private final KafkaConfigProperties kafkaConfigProperties;

    public KafkaAdminConfig(KafkaConfigProperties kafkaConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigProperties.bootstrapServers())
        );
    }
}