package org.etux.kafka.deadletter

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.UUID
import kotlin.time.measureTimedValue

class DeadLetterProcessor<K, V>(
    private val deadLetterStoreName: String,
    private val reprocessIntervalInSeconds: Long,
    private val maxRetries: Int,
    private val processingMode: Mode,
    private val businessLogic: (key: K, value: V, headers: Map<String, String>) -> Unit,
): Processor<K, V, K, List<DeadLetteredValue<V>>> {

    enum class Mode {
        ORDERED,
        UNORDERED
    }

    private lateinit var processorContext: ProcessorContext<K, List<DeadLetteredValue<V>>>
    private lateinit var stateStore: KeyValueStore<K, List<DeadLetteredValue<V>>>
    private val logger = LoggerFactory.getLogger(DeadLetterProcessor::class.java)

    override fun init(context: ProcessorContext<K, List<DeadLetteredValue<V>>>?) {
        processorContext = context ?: throw IllegalArgumentException("ProcessorContext cannot be null")
        stateStore = context.getStateStore(deadLetterStoreName)

        processorContext.schedule(
            /* interval = */ Duration.ofSeconds(reprocessIntervalInSeconds),
            /* type = */ PunctuationType.WALL_CLOCK_TIME)
        { _ ->
            stateStore.all().forEach(::reprocess)
        }
    }

    override fun process(record: Record<K, V>) {
        val key = record.key()
        val value = record.value()
        val headers = record.headers().associate {
            it.key().toString() to it.value().decodeToString()
        }
        val uniqueMessageId = UUID.randomUUID()

        val (deadLetteredMessagesForKey, timeTaken) = measureTimedValue {
            stateStore.get(key)
        }
        logger.info("Retrieved dead-lettered messages for key '$key' in ${timeTaken.inWholeMicroseconds} microseconds")


        if (deadLetteredMessagesForKey != null) {
            logger.info("Dead-lettered messages found for key '$key': '$deadLetteredMessagesForKey'")
            if (processingMode == Mode.ORDERED) {
                addDeadLetter(
                    deadLetteredMessagesForKey = deadLetteredMessagesForKey,
                    uniqueMessageId = uniqueMessageId,
                    value = value,
                    key = key,
                    cause = "Message with same key already dead-lettered and processing mode is '$processingMode'",
                    headers = headers
                )

                logger.info("Message with key '$key' and value '$value' added to dead-letter store with unique ID '$uniqueMessageId'. Will not process further.")

                processorContext.commit()
                return
            }
        }

        try {
            businessLogic(
                record.key(),
                record.value(),
                headers
            )
        } catch (runtimeException: RuntimeException) {
            addDeadLetter(
                deadLetteredMessagesForKey = deadLetteredMessagesForKey,
                uniqueMessageId = uniqueMessageId,
                value = value,
                key = key,
                cause = runtimeException.message,
                stackTrace = runtimeException.stackTraceToString(),
                headers = headers
            )
        }
        processorContext.commit()
    }

    private fun addDeadLetter(
        deadLetteredMessagesForKey: List<DeadLetteredValue<V>>?,
        uniqueMessageId: UUID,
        key: K,
        value: V,
        cause: String? = null,
        stackTrace: String? = null,
        headers: Map<String, String> = emptyMap(),
    ) {
        val newDeadLetterValue = DeadLetteredValue(
            retryCount = 1,
            uniqueMessageId = uniqueMessageId,
            payload = value,
            cause = cause,
            stackTrace = stackTrace,
            headers = headers,
        )

        when (deadLetteredMessagesForKey) {
            null -> listOf(newDeadLetterValue)
            else -> deadLetteredMessagesForKey + newDeadLetterValue
        }.also {
            stateStore.put(
                /* key = */ key,
                /* value = */ it,
            )
        }
    }

    private fun reprocess(keyValue: KeyValue<K, List<DeadLetteredValue<V>>>) {
        val key = keyValue.key
        val messages = keyValue.value

        when (processingMode) {
            Mode.ORDERED -> {
                val firstMessage = messages.firstOrNull()

                firstMessage?.let {
                    logger.info("Reprocessing dead-lettered messages from state store '${stateStore.name()}' for key: '$key' in order")
                    reprocessSingle(
                        key = key,
                        message = firstMessage,
                        messages = messages
                    )
                }
            }
            Mode.UNORDERED -> {
                logger.info("Reprocessing dead-lettered messages from state store '${stateStore.name()}' for key: '$key' out of order")
                messages.forEach { message ->
                    reprocessSingle(
                        key = key,
                        message = message,
                        messages = messages
                    )
                }
            }
        }
    }

    private fun reprocessSingle(
        key: K,
        message: DeadLetteredValue<V>,
        messages: List<DeadLetteredValue<V>>,
    ) {
        if (message.retryCount >= maxRetries) {
            logger.info("Skipping reprocessing of dead-lettered message with key: '$key' and value: '$message' due to max retry attempts reached")
            return
        }

        logger.info("Reprocessing dead-lettered message with key: '$key' and value: '$message'")

        try {
            businessLogic(key, message.payload, message.headers)
            logger.info("Successfully reprocessed dead-lettered message with key: '${key}' and value: '${message}'")

            stateStore.put(
                /* key = */ key,
                /* value = */ messages.filter { otherMessage ->
                    otherMessage.uniqueMessageId != message.uniqueMessageId
                }
            )
        } catch (runtimeException: RuntimeException) {
            logger.error(
                "Failed to reprocess dead-lettered message with key: '$key' and value: '${message.payload}'",
                runtimeException
            )

            stateStore.put(
                /* key = */ key,
                /* value = */ messages.map { otherMessage ->
                    if (otherMessage.uniqueMessageId == message.uniqueMessageId) {
                        otherMessage.copy(retryCount = otherMessage.retryCount + 1)
                    } else {
                        otherMessage
                    }
                }
            )
        }
        processorContext.commit()
    }
}
