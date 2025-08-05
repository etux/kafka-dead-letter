package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.etux.kafka.ExampleKafkaMessage
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage
import org.etux.kafka.deadletter.statestore.DeadLetteredMessageListSerde
import org.slf4j.LoggerFactory
import java.util.Properties

class DeadLetteringStream(
    private val inputTopic: String,
    private val inputStore: String,
) {
    val topology: Topology by lazy { createTopology() }

    private fun createTopology(): Topology {
        val builder = StreamsBuilder()

        val storeBuilder: StoreBuilder<KeyValueStore<String, List<DeadLetteredMessage<String>>>> = Stores.keyValueStoreBuilder(
            /* supplier = */ Stores.inMemoryKeyValueStore(inputStore),
            /* keySerde = */ Serdes.String(),
            /* valueSerde = */ DeadLetteredMessageListSerde(),
        )
        builder.addStateStore(storeBuilder)

        val deadLetterStream: KStream<String, String> = builder.stream(
            /* topic = */ inputTopic,
            /* consumed = */ Consumed.with(
                /* keySerde = */ Serdes.String(),
                /* valueSerde = */ Serdes.String(),
            ).withName(inputStore),
        )

        deadLetterStream
            .peek { key, value -> logger.info("Received message. Key: $key, Value: $value") }
            .process(
                /* processorSupplier = */ DeadLetterProcessorSupplier(inputStore),
                /* ...stateStoreNames = */ inputStore,
            )
            .groupByKey()

        return builder.build()
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DeadLetteringStream::class.java)
    }
}
