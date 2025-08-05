package org.etux.kafka.deadletter.statestore

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class DeadLetteredMessageListSerde<T> : Serde<List<DeadLetteredMessage<T>>> {
    private val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    override fun serializer() =  Serializer<List<DeadLetteredMessage<T>>> { _, data ->
        objectMapper.writeValueAsBytes(data)
    }

    override fun deserializer() = Deserializer { _, bytes ->
        objectMapper.readValue<List<DeadLetteredMessage<T>>>(bytes)
    }
}
