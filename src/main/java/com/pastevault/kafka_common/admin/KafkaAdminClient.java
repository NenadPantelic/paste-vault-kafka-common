package com.pastevault.kafka_common.admin;

import com.pastevault.kafka_common.exception.KafkaClientException;
import com.pastevault.kafka_common.properties.KafkaConfigProperties;
import com.pastevault.kafka_common.properties.RetryConfigProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Component
public class KafkaAdminClient {

    private final KafkaConfigProperties kafkaConfigProperties;
    private final RetryConfigProperties retryConfigProperties;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;


    public KafkaAdminClient(KafkaConfigProperties kafkaConfigProperties,
                            RetryConfigProperties retryConfigProperties,
                            AdminClient adminClient,
                            RetryTemplate retryTemplate) {
        this.kafkaConfigProperties = kafkaConfigProperties;
        this.retryConfigProperties = retryConfigProperties;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
    }

    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable throwable) {
            throw new KafkaClientException("Reached max number of retries when trying to create Kafka topics.");
        }

        checkIfTopicsCreated();
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicsToCreate = kafkaConfigProperties.topicNamesToCreate();
        log.info("Creating topics {}, attempt {}", topicsToCreate, retryContext.getRetryCount());

        List<NewTopic> topics = topicsToCreate.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigProperties.numOfPartitions(),
                kafkaConfigProperties.replicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(topics);
    }

    public void checkIfTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigProperties.maxAttempts();
        int multiplier = retryConfigProperties.multiplier().intValue();
        Long sleepTimeInMs = retryConfigProperties.sleepTimeMs();

        for (String topicToCreate : kafkaConfigProperties.topicNamesToCreate()) {
            while (!isTopicCreated(topics, topicToCreate)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeInMs);
                sleepTimeInMs += multiplier;
                topics = getTopics();
            }
        }
    }

    private void sleep(Long sleepTimeInMs) {
        try {
            Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping.");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) {
        if (retry > maxRetry) {
            throw new KafkaClientException("Reached max number of retry for reading Kafka topic(s)");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null) {
            return false;
        }

        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }

    private Collection<TopicListing> getTopics() {
        try {
            return retryTemplate.execute(this::doGetTopics);
        } catch (Throwable throwable) {
            throw new KafkaClientException("Reached max number of retries when reading Kafka topic(s)!");
        }
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info(
                "Reading Kafka topic {}, attempt {}",
                kafkaConfigProperties.topicNamesToCreate(), retryContext.getRetryCount()
        );
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if (topics != null) {
            topics.forEach(topic -> log.info("Topic with name {}", topic.name()));
        }

        return topics;
    }
}
