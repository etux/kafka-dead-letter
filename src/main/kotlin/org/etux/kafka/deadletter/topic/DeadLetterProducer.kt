package org.etux.kafka.deadletter.topic

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import java.util.Properties

class DeadLetterProducer<K, V>(
    bootstrapServer: String,
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>,
    private val deadLetterTopic: String,
): KafkaProducer<K, V>(
    Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer::class.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }
) {
    fun publishDeadLetter(key: K, value: V, operation: DeadLetterMessageType, uniqueMessageId: String) {
        val producerRecord = ProducerRecord(
            /* topic = */ deadLetterTopic,
            /* key = */ key,
            /* value = */ value,
        )

        producerRecord.headers().apply {
            add("operation", operation.name.toByteArray())
            add("unique-message-id", uniqueMessageId.toByteArray())
        }

        send(producerRecord)
    }
}
