package com.pastevault.kafka_common.config;

import com.pastevault.kafka_common.properties.KafkaConfigProperties;
import com.pastevault.kafka_common.properties.KafkaProducerConfigProperties;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig<K extends Serializable, V extends SpecificRecordBase> {

    private final KafkaConfigProperties kafkaConfigProperties;
    private final KafkaProducerConfigProperties kafkaProducerConfigProperties;

    public KafkaProducerConfig(KafkaConfigProperties kafkaConfigProperties,
                               KafkaProducerConfigProperties kafkaProducerConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        this.kafkaProducerConfigProperties = kafkaProducerConfigProperties;
    }

    @Bean
    public Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigProperties.bootstrapServers());
        props.put(kafkaConfigProperties.schemaRegistryUrlKey(), kafkaConfigProperties.schemaRegistryUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigProperties.keySerializerClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerConfigProperties.valueSerializerClass());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerConfigProperties.requestsTimeoutMs());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfigProperties.retryCount());
        return props;
    }

    @Bean
    public ProducerFactory<K, V> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<K, V> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
