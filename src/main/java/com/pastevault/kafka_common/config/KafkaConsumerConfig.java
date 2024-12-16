package com.pastevault.kafka_common.config;

import com.pastevault.kafka_common.properties.KafkaConfigProperties;
import com.pastevault.kafka_common.properties.KafkaConsumerConfigProperties;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@EnableKafka // enables detection of KafkaListener annotation
@Configuration
public class KafkaConsumerConfig<K extends Serializable, V extends SpecificRecordBase> {

    private final KafkaConfigProperties kafkaConfigProperties;
    private final KafkaConsumerConfigProperties kafkaConsumerConfigProperties;

    public KafkaConsumerConfig(KafkaConfigProperties kafkaConfigProperties,
                               KafkaConsumerConfigProperties kafkaConsumerConfigProperties) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        this.kafkaConsumerConfigProperties = kafkaConsumerConfigProperties;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigProperties.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConsumerConfigProperties.keyDeserializerClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConsumerConfigProperties.valueDeserializerClass());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerConfigProperties.consumerGroupId());
        props.put(kafkaConfigProperties.schemaRegistryUrlKey(), kafkaConfigProperties.schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        return props;
    }

    @Bean
    public ConsumerFactory<K, V> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<K, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(kafkaConsumerConfigProperties.concurrencyLevel());
        factory.getContainerProperties().setPollTimeout(kafkaConsumerConfigProperties.pollTimeoutInMillis());
        return factory;
    }
}