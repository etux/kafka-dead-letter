package org.etux.kafka.deadletter

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
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
        val deadLetterStream: KStream<String, DeadLetterRecord<String, String>> = builder.stream(
            inputTopic,
            Consumed.with(Serdes.String(), DeadLetterJacksonSerde<String, String>()).withName(inputStore),
        )

        deadLetterStream
            .filterNot { key, value ->
                when(ApplicationStream.MessageType.valueOf(value.value)) {
                    ApplicationStream.MessageType.SUCCESSFUL -> false
                    ApplicationStream.MessageType.RETRY -> false
                    else -> true
                }
            }

        // Group by key and aggregate into a list per key
        val deadLetterTable: KTable<String, List<DeadLetterRecord<String, String>>> = deadLetterStream
            .peek { key, value -> logger.info("Received message. Key: $key, Value: $value") }
            .groupByKey()
            .aggregate(
                { emptyList() },
                { key: String, value: DeadLetterRecord<String, String>, aggregate: List<DeadLetterRecord<String, String>> ->
                    logger.info("Adding to dead letter messages bucket $key of size ${aggregate.size} record: $value")
                    aggregate + value
                },
                Materialized.`as`<String, List<DeadLetterRecord<String, String>>>(Stores.inMemoryKeyValueStore(inputStore))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(DeadLetterListJacksonSerde(String::class.java, String::class.java)),
            )
        return builder.build()
    }

    fun run() {
        val streams = KafkaStreams(topology, properties)
        streams.start()
        Runtime
            .getRuntime()
            .addShutdownHook(Thread { streams.close() })
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DeadLetterStream::class.java)
    }
}