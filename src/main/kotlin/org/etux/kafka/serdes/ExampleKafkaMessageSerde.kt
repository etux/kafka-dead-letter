package org.etux.kafka.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.etux.kafka.ExampleKafkaMessage

class ExampleKafkaMessageSerde: Serde<ExampleKafkaMessage> {
    private val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    override fun serializer() = Serializer<ExampleKafkaMessage> { _, data ->
        objectMapper.writeValueAsBytes(data)
    }
    override fun deserializer() = Deserializer { _, bytes ->
        objectMapper.readValue<ExampleKafkaMessage>(bytes)
    }
}
