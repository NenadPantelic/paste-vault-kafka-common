package com.pastevault.kafka_common.event;

import org.apache.avro.specific.SpecificRecordBase;

import java.io.Serializable;

public interface KafkaConsumer<K extends Serializable, V extends SpecificRecordBase> {

    void receive(V message, String key, Integer partition, Long offset);
}