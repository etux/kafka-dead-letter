package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import java.util.Properties

class DeadLetterKafkaStream<K, V>(
    private val properties: Properties,
    private val topicName: String,
    private val deadLetterStoreName: String,
    private val reprocessIntervalInSeconds: Long,
    private val processingMode: DeadLetterProcessor.Mode,
    private val businessLogic: (key: K, value: V) -> Unit,
) {
    private val logger = LoggerFactory.getLogger(DeadLetterKafkaStream::class.java)

    fun createStream(): KafkaStreams {
        val builder = StreamsBuilder()

        val storeBuilder: StoreBuilder<KeyValueStore<String, List<DeadLetteredValue<String>>>> = Stores.keyValueStoreBuilder(
            /* supplier = */ Stores.inMemoryKeyValueStore(deadLetterStoreName),
            /* keySerde = */ Serdes.String(),
            /* valueSerde = */ DeadLetteredValueListSerde(),
        )
        builder.addStateStore(storeBuilder)

        val deadLetterStream: KStream<K, V> = builder.stream(
            /* topic = */ topicName,
            /* consumed = */ Consumed.with(
                /* keySerde = */ Serdes.String(),
                /* valueSerde = */ Serdes.String(),
            ).withName("$topicName-dead-letter-stream"),
        ) as KStream<K, V> // TODO: Fix generics, the serdes should be parameterized

        val deadLetterProcessorSupplier = DeadLetterProcessorSupplier(
            deadLetterStoreName = deadLetterStoreName,
            reprocessIntervalInSeconds = reprocessIntervalInSeconds,
            processingMode = processingMode,
            businessLogic = businessLogic,
        )

        deadLetterStream
            .peek { key, value -> logger.info("Received message with key: '$key' and value: '$value'") }
            .process(
                /* processorSupplier = */ deadLetterProcessorSupplier,
                /* ...stateStoreNames = */ deadLetterStoreName,
            )

        return KafkaStreams(
            /* topology = */ builder.build(),
            /* props = */ properties,
        )
    }
}