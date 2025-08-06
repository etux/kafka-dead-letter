package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.Properties

class DeadLetterKafkaStream<K, V>(
    private val properties: Properties,
    private val topicName: String,
    private val deadLetterStoreName: String,
    private val reprocessIntervalInSeconds: Long,
    private val processingMode: DeadLetterProcessor.Mode,
    private val keySerde: Serde<K>,
    private val valueSerde: Serde<V>,
    private val businessLogic: (key: K, value: V, headers: Map<String, String>) -> Unit,
) where K: Any, V: Serializable {
    private val logger = LoggerFactory.getLogger(DeadLetterKafkaStream::class.java)

    fun createStream(): KafkaStreams {
        val builder = StreamsBuilder()

        builder.addStateStore(
            Stores.keyValueStoreBuilder(
                /* supplier = */ Stores.inMemoryKeyValueStore(deadLetterStoreName),
                /* keySerde = */ keySerde,
                /* valueSerde = */ DeadLetteredValueListSerde<V>(),
            ).withCachingEnabled()
        )

        builder.stream(
            /* topic = */ topicName,
            /* consumed = */ Consumed.with(
                /* keySerde = */ keySerde,
                /* valueSerde = */ valueSerde,
            ).withName("$topicName-dead-letter-stream"),
        ).peek { key, value -> logger.info("Received message with key: '$key' and value: '$value'") }
        .process(
            /* processorSupplier = */ DeadLetterProcessorSupplier(
                deadLetterStoreName = deadLetterStoreName,
                reprocessIntervalInSeconds = reprocessIntervalInSeconds,
                processingMode = processingMode,
                businessLogic = businessLogic,
            ),
            /* ...stateStoreNames = */ deadLetterStoreName,
        )

        return KafkaStreams(
            /* topology = */ builder.build(),
            /* props = */ properties,
        )
    }
}