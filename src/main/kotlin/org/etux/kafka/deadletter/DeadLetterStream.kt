package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.Stores
import org.etux.kafka.deadletter.serdes.DeadLetterJacksonSerde
import org.etux.kafka.deadletter.serdes.DeadLetterListJacksonSerde
import org.slf4j.LoggerFactory
import java.util.Properties

class DeadLetterStream(
    private val inputTopic: String,
    private val inputStore: String,
    private val properties: Properties,
) {

    val topology: Topology by lazy { topology() }

    private fun topology(): Topology {
        val builder = StreamsBuilder()
        val deadLetterStream: KStream<String, String> = builder.stream(
            /* topic = */ inputTopic,
            /* consumed = */ Consumed.with(
                /* keySerde = */ Serdes.String(),
                /* valueSerde = */ Serdes.String(),
            ).withName(inputStore),
        )

        deadLetterStream
            .filter { _, value ->
                when(ExampleKafkaApplication.MessageType.valueOf(value)) {
                    ExampleKafkaApplication.MessageType.SUCCESSFUL, ExampleKafkaApplication.MessageType.RETRY -> true
                    else -> false
                }
            }

        // Group by key and aggregate into a list per key
        deadLetterStream
            .peek { key, value -> logger.info("Received message. Key: $key, Value: $value") }
            .groupByKey()
            .aggregate(
                /* p0 = */ { emptyList() },
                /* p1 = */ { key: String, value: String, aggregate: List<String> ->
                    logger.info("Adding to dead letter messages bucket $key of size ${aggregate.size} record: $value")
                    aggregate + value
                },
                /* p2 = */ Materialized.`as`<String, List<String>>(Stores.inMemoryKeyValueStore(inputStore))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(object: Serde<List<String>> { // TODO extract serde
                        override fun serializer() = Serializer<List<String>> { _, data ->
                            data?.joinToString(separator = ",")?.toByteArray()
                        }
                        override fun deserializer() = Deserializer { _, bytes ->

                            bytes?.toString(Charsets.UTF_8)?.split(",") ?: emptyList()
                        }
                    }),
            )

        return builder.build()
    }

    fun run() {
        val streams = KafkaStreams(
            /* topology = */ topology,
            /* props = */ properties,
        )

        streams.start()

        Runtime
            .getRuntime()
            .addShutdownHook(Thread { streams.close() })
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DeadLetterStream::class.java)
    }
}
