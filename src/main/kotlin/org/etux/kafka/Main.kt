package org.etux.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.etux.kafka.deadletter.DeadLetterReprocessor
import org.etux.kafka.deadletter.DeadLetteringStream
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage
import org.etux.kafka.deadletter.topic.DeadLetterProducer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Executors

private const val APPLICATION_ID = "dead-letter"
private val logger = LoggerFactory.getLogger("MainKt")

fun main() {
    val bootstrapServer = "localhost:9092"
    val topic = "my-topic"
    val deadLetterTopic = "my-topic-dlt"
    val stateStoreName = "my-state-store"
    val processingMode = ProcessingMode.DELTA

    TopicCreator(
        bootstrapServer = bootstrapServer,
        topics = listOf(
            topic,
            deadLetterTopic,
            "$APPLICATION_ID-my-state-store-changelog",
        ),
    ).apply { run() }

    val deadLetteringStream = DeadLetteringStream(
        inputTopic = deadLetterTopic,
        inputStore = stateStoreName,
    )
    val topology = deadLetteringStream.topology

    val streamProperties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30_000) // TODO: test
    }

    val kafkaStreams = KafkaStreams(
        /* topology = */ topology,
        /* props = */ streamProperties,
    ).also {
        Runtime.getRuntime().addShutdownHook(Thread { it.close() })
        it.start()

        while (it.state() != KafkaStreams.State.RUNNING) {
            logger.warn("Waiting for Kafka Streams to start...")
            Thread.sleep(1000)
        }
    }

    val deadLetterStateStore = kafkaStreams.store<ReadOnlyKeyValueStore<String, List<DeadLetteredMessage<String>>>>(
        StoreQueryParameters.fromNameAndType(
            /* storeName = */ stateStoreName,
            /* queryableStoreType = */ QueryableStoreTypes.keyValueStore()
        )
    )

    val businessLogic: (String) -> Boolean = { message ->
        logger.info("Business logic running for message: $message")

        when (ExampleKafkaMessage.MessageType.valueOf(message)) {
            ExampleKafkaMessage.MessageType.SUCCESSFUL -> {
                logger.info("Message processed successfully: $message")
                true
            }
            ExampleKafkaMessage.MessageType.RETRY -> {
                logger.warn("Message requires retry: $message")
                false
            }
            ExampleKafkaMessage.MessageType.FAIL_FOREVER -> {
                logger.error("Message failed permanently: $message")
                false
            }
        }
    }

    val deadLetterProducer = DeadLetterProducer<String, String>(
        bootstrapServer = bootstrapServer,
        keySerializer = StringSerializer(),
        valueSerializer = StringSerializer()
    )

    val deadLetterReprocessor = DeadLetterReprocessor(
        processingMode = processingMode,
        deadLetterStateStore = deadLetterStateStore,
        periodInSeconds = 1L,
        deadLetterProducer = deadLetterProducer,
        deadLetterTopic = deadLetterTopic,
    ) { message ->
        when (ExampleKafkaMessage.MessageType.valueOf(message)) {
            ExampleKafkaMessage.MessageType.SUCCESSFUL -> {
                logger.info("Reprocessing successful message: $message")
                true
            }
            ExampleKafkaMessage.MessageType.RETRY -> {
                logger.info("Reprocessing message that requires retry: $message")
                true
            }
            ExampleKafkaMessage.MessageType.FAIL_FOREVER -> {
                logger.error("Reprocessing failed message permanently: $message")
                false
            }
        }
    }

    val exampleConsumerThatIsAbleToDeadLetter = ExampleConsumerThatIsAbleToDeadLetter(
        bootstrapServer = bootstrapServer,
        deadLetterProducer = deadLetterProducer,
        deadLetterTopic = deadLetterTopic,
        businessLogic = businessLogic,
        processingMode = processingMode,
        deadLetterStateStore = deadLetterStateStore
    )

    val exampleKafkaApplication = ExampleKafkaApplication(
        inputTopic = topic,
        consumer = exampleConsumerThatIsAbleToDeadLetter,
    )

    Executors.newScheduledThreadPool(3).also {
//        it.submit(deadLetteringStream::run)
        it.submit(deadLetterReprocessor::run)
        it.submit(exampleKafkaApplication::run)
    }
}
