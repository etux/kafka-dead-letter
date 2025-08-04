package org.etux.kafka.deadletter.serdes

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.etux.kafka.deadletter.DeadLetterRecord
import org.etux.kafka.deadletter.PutDeadLetterCommand
import org.junit.jupiter.api.Test

class DeadLetterJacksonSerdeTest {

    @Test
    fun test(){
        val objectMapper = jacksonObjectMapper()
        val key = 1L
        val value = "value"
        val original = PutDeadLetterCommand(
            deadLetter = DeadLetterRecord(
                key = key,
                value = value
            )
        )

        val bytes = objectMapper.writeValueAsBytes(original)
        val serialized: PutDeadLetterCommand<Long, String> = objectMapper.readValue(
            bytes,
            object : TypeReference<PutDeadLetterCommand<Long, String>>() {}
        )

        assertThat(serialized).isEqualTo(original)
    }

}