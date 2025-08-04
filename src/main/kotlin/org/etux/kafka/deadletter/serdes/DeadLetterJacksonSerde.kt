package org.etux.kafka.deadletter.serdes

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.etux.kafka.deadletter.DeadLetterCommand
import org.etux.kafka.deadletter.DeadLetterRecord

class MyInputDeadLetterCommandSerializer : Serializer<DeadLetterCommand<String, String>> {

    private val objectMapper = jacksonObjectMapper()

    override fun serialize(
        topic: String?,
        data: DeadLetterCommand<String, String>?
    ): ByteArray? {
        return data?.let { objectMapper.writeValueAsBytes(data) }
    }
}

class MyInputDeadLetterCommandDeserializer : Deserializer<DeadLetterCommand<String, String>> {

    private val objectMapper = jacksonObjectMapper()
    override fun deserialize(
        topic: String?,
        data: ByteArray?
    ): DeadLetterCommand<String, String>? {
        return data
            ?.let {
                objectMapper.readValue(
                    it,
                    object : TypeReference<DeadLetterCommand<String, String>>(){}
                )
            }
    }

}

class MyInputDeadLetterCommandSerde : Serde<DeadLetterCommand<String, String>> {
    private val objectMapper = jacksonObjectMapper()

    override fun deserializer() = Deserializer<DeadLetterCommand<String, String>?> { topic, bytes ->
        bytes?.let { objectMapper.readValue(it, object: TypeReference<DeadLetterCommand<String, String>>(){}) }
    }

    override fun serializer() = Serializer<DeadLetterCommand<String, String>> { topic, data ->
        objectMapper.writeValueAsBytes(data)
    }
}

abstract class DeadLetterCommandJacksonSerde<K, V> : Serde<DeadLetterCommand<K,V>>{
    private val objectMapper = jacksonObjectMapper()

    override fun deserializer() = Deserializer<DeadLetterCommand<K, V>?> { topic, bytes ->
        bytes?.let { objectMapper.readValue(it, object: TypeReference<DeadLetterCommand<K, V>>(){})}
    }

    override fun serializer() = Serializer<DeadLetterCommand<K, V>?> { topic, command ->
        objectMapper.writeValueAsBytes(command)
    }
}


class DeadLetterJacksonSerde<K,V>() : Serde<DeadLetterRecord<K, V>>{
    private val objectMapper = jacksonObjectMapper()

    override fun serializer(): Serializer<DeadLetterRecord<K,V>> = Serializer<DeadLetterRecord<K, V>> { topic, data ->
        data?.let { objectMapper.writeValueAsBytes(it) }
    }

    override fun deserializer(): Deserializer<DeadLetterRecord<K, V>> =
        Deserializer<DeadLetterRecord<K, V>> { topic, data ->
            data?.let { objectMapper.readValue(it, object: TypeReference<DeadLetterRecord<K, V>>(){}) }
        }
}

class DeadLetterListJacksonSerde<K,V>(
    keyClass: Class<K>,
    valueClass: Class<V>,
) : Serde<List<DeadLetterRecord<K,V>>> {
    private val objectMapper = jacksonObjectMapper()

    private val javaType = objectMapper.typeFactory.constructCollectionType(
        List::class.java,
        objectMapper.typeFactory.constructParametricType(
            DeadLetterRecord::class.java,
            keyClass,
            valueClass,
        )
    )

    override fun serializer(): Serializer<List<DeadLetterRecord<K,V>>> = Serializer<List<DeadLetterRecord<K, V>>> { topic, data ->
        data?.let { objectMapper.writeValueAsBytes(it) }
    }

    override fun deserializer(): Deserializer<List<DeadLetterRecord<K, V>>> = Deserializer<List<DeadLetterRecord<K,V>>> { topic, bytes ->
        bytes?.let { objectMapper.readValue(it, javaType) }
    }
}