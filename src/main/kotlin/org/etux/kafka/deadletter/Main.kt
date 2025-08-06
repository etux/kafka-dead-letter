package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Executors

private const val APPLICATION_ID = "my-application"
private val logger = LoggerFactory.getLogger("org.etux.kafka.deadletter.MainKt")

enum class MessageType {
    SUCCESSFUL,
    RETRY,
    FAIL_FOREVER,
}

fun main() {
    val bootstrapServer = "localhost:9092"
    val topic = "my-topic"
    val deadLetterStoreName = "$topic-dead-letter-state-store"
    val processingMode = DeadLetterProcessor.Mode.UNORDERED

    TopicCreator(
        bootstrapServer = bootstrapServer,
        topics = listOf(topic),
    ).run()

    val deadLetterKafkaStream = DeadLetterKafkaStream<String, String>(
        properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30_000)
        },
        topicName = topic,
        deadLetterStoreName = deadLetterStoreName,
        reprocessIntervalInSeconds = 5L,
        processingMode = processingMode,
    ) { key, value ->
        logger.info("Business logic executing for message with key: '$key' and value: '$value'")

        val type = MessageType.valueOf(value)

        when (type) {
            MessageType.RETRY -> throw RuntimeException("Simulated retry for message with key: '$key'")
            MessageType.SUCCESSFUL -> logger.info("Successfully processed message with key: '$key'")
            MessageType.FAIL_FOREVER -> throw RuntimeException("Simulated failure for message with key: '$key'")
        }
    }

    val deadLetteringKafkaStream = deadLetterKafkaStream.createStream()

    Executors.newScheduledThreadPool(3).also {
        deadLetteringKafkaStream.start()

        while (deadLetteringKafkaStream.state() != KafkaStreams.State.RUNNING) {
            logger.info("Waiting for Kafka Streams to start...")
            Thread.sleep(1000)
        }
    }
}
