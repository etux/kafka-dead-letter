package org.etux.kafka.deadletter.topic

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serializer
import java.util.Properties

class DeadLetterProducer<K, V>(
    bootstrapServer: String,
    keySerializer: Serializer<K>,
    valueSerializer: Serializer<V>
): KafkaProducer<K, V>(
    Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer::class.java)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer::class.java)
        put(ProducerConfig.ACKS_CONFIG, "all")
    }
)