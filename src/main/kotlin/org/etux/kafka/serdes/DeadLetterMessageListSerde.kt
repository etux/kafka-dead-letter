package org.etux.kafka.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.etux.kafka.deadletter.DeadLetterMessage

class DeadLetterMessageListSerde : Serde<List<DeadLetterMessage>> {
    private val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    override fun serializer() =  Serializer<List<DeadLetterMessage>> { _, data ->
        objectMapper.writeValueAsBytes(data)
    }

    override fun deserializer() = Deserializer<List<DeadLetterMessage>> { _, bytes ->
        objectMapper.readValue<List<DeadLetterMessage>>(bytes)
    }
}
