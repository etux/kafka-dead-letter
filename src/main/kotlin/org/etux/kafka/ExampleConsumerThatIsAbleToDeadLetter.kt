package org.etux.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.etux.kafka.deadletter.statestore.DeadLetteredMessage
import org.etux.kafka.deadletter.topic.DeadLetterMessageType
import org.etux.kafka.deadletter.topic.DeadLetterProducer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.UUID

class ExampleConsumerThatIsAbleToDeadLetter<K, V>(
    bootstrapServer: String,
    private val processingMode: ProcessingMode,
    private val deadLetterStateStore: ReadOnlyKeyValueStore<K, List<DeadLetteredMessage<V>>>,
    private val deadLetterProducer: DeadLetterProducer<K, V>,
    private val deadLetterTopic: String,
    private val businessLogic: (V) -> Boolean,
): KafkaConsumer<K, V>(Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(ConsumerConfig.GROUP_ID_CONFIG, "my-application")
    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
}) {

    private val logger = LoggerFactory.getLogger(ExampleConsumerThatIsAbleToDeadLetter::class.java)

    override fun poll(timeout: Duration): ConsumerRecords<K, V> {
        val poll = super.poll(timeout)

        poll.forEach { record ->
            val key = record.key()
            val value = record.value()

            if (processingMode == ProcessingMode.DELTA && deadLetterStateStore.get(key) != null) {
                logger.info("Dead-letter state store contains message with key: '$key' and value: '$value'. Appending to dead-letter.")

                publishDeadLetter(
                    key = key,
                    value = value,
                )
            }

            if (!businessLogic(value)) {
                logger.warn("Message with key: $key and value: $value failed processing, sending to dead-letter topic.")

                publishDeadLetter(
                    key = key,
                    value = value,
                )
            }
        }

        return poll
    }

    // TODO: refactor outside so it can be reused
    private fun publishDeadLetter(key: K, value: V) {
        val producerRecord = ProducerRecord(
            /* topic = */ deadLetterTopic,
            /* key = */ key,
            /* value = */ value,
        )

        producerRecord.headers().apply {
            add("operation", DeadLetterMessageType.PUT.name.toByteArray())
            add("unique-message-id", UUID.randomUUID().toString().toByteArray())
        }

        deadLetterProducer.send(producerRecord)
    }
}