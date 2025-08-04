package org.etux.kafka.deadletter

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName

@JsonSubTypes(
    JsonSubTypes.Type(PutDeadLetterCommand::class),
    JsonSubTypes.Type(DeleteDeadLetterCommand::class),
    JsonSubTypes.Type(RetryDeadLetterCommand::class),
)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "commandType"
)
sealed interface DeadLetterCommand<K,V> {
    val commandType: DeadLetterCommandType
    val deadLetter: DeadLetterRecord<K, V>
}

@JsonTypeName("PUT")
data class PutDeadLetterCommand<K,V>(
    override val commandType: DeadLetterCommandType = DeadLetterCommandType.PUT,
    override val deadLetter: DeadLetterRecord<K,V>
) : DeadLetterCommand<K,V>

@JsonTypeName("DELETE")
data class DeleteDeadLetterCommand<K,V>(
    override val commandType: DeadLetterCommandType = DeadLetterCommandType.DELETE,
    override val deadLetter: DeadLetterRecord<K, V>
) : DeadLetterCommand<K,V>

@JsonTypeName("RETRY")
data class RetryDeadLetterCommand<K,V>(
    override val commandType: DeadLetterCommandType = DeadLetterCommandType.RETRY,
    override val deadLetter: DeadLetterRecord<K, V>
) : DeadLetterCommand<K, V>
