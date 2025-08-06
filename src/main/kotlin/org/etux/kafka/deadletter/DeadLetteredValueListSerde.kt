package org.etux.kafka.deadletter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class DeadLetteredValueListSerde<V> : Serde<List<DeadLetteredValue<V>>> {
    private val objectMapper = ObjectMapper().apply {
        registerModule(KotlinModule.Builder().build())
    }

    override fun serializer() =  Serializer<List<DeadLetteredValue<V>>> { _, data ->
        objectMapper.writeValueAsBytes(data)
    }

    override fun deserializer() = Deserializer { _, bytes ->
        objectMapper.readValue<List<DeadLetteredValue<V>>>(bytes)
    }
}
