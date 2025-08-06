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

class DeadLetterProcessor<K, V>(
    private val deadLetterStoreName: String,
    private val reprocessIntervalInSeconds: Long,
    private val maxRetries: Int,
    private val processingMode: Mode,
    private val businessLogic: (key: K, value: V) -> Unit,
): Processor<K, V, K, List<DeadLetteredValue<V>>> {

    enum class Mode {
        ORDERED,
        UNORDERED
    }

    private lateinit var processorContext: ProcessorContext<K, List<DeadLetteredValue<V>>>
    private lateinit var keyValueStore: KeyValueStore<K, List<DeadLetteredValue<V>>>
    private val logger = LoggerFactory.getLogger(DeadLetterProcessor::class.java)

    override fun init(context: ProcessorContext<K, List<DeadLetteredValue<V>>>?) {
        processorContext = context ?: throw IllegalArgumentException("ProcessorContext cannot be null")
        keyValueStore = context.getStateStore(deadLetterStoreName) as KeyValueStore<K, List<DeadLetteredValue<V>>>

        processorContext.schedule(
            /* interval = */ Duration.ofSeconds(reprocessIntervalInSeconds),
            /* type = */ PunctuationType.WALL_CLOCK_TIME)
        { _ ->
            keyValueStore.all().forEach(::reprocess)
        }
    }

    override fun process(record: Record<K, V>) {
        val key = record.key()
        val value = record.value()
        val uniqueMessageId = UUID.randomUUID()

        val deadLetteredMessagesForKey = keyValueStore.get(key)

        if (deadLetteredMessagesForKey != null) {
            logger.info("Dead-lettered messages found for key '$key': '$deadLetteredMessagesForKey'")
            if (processingMode == Mode.ORDERED) {
                addDeadLetter(
                    deadLetteredMessagesForKey = deadLetteredMessagesForKey,
                    uniqueMessageId = uniqueMessageId,
                    value = value,
                    key = key,
                )

                logger.info("Message with key '$key' and value '$value' added to dead-letter store with unique ID '$uniqueMessageId'. Will not process further.")

                processorContext.commit()
                return
            }
        }

        try {
            businessLogic(
                record.key(),
                record.value()
            )
        } catch (runtimeException: RuntimeException) {
            addDeadLetter(
                deadLetteredMessagesForKey = deadLetteredMessagesForKey,
                uniqueMessageId = uniqueMessageId,
                value = value,
                key = key
            )
        }
        processorContext.commit()
    }

    private fun addDeadLetter(
        deadLetteredMessagesForKey: List<DeadLetteredValue<V>>?,
        uniqueMessageId: UUID,
        value: V,
        key: K
    ) {
        when (deadLetteredMessagesForKey) {
            null -> listOf(
                DeadLetteredValue(
                    retryCount = 0,
                    uniqueMessageId = uniqueMessageId,
                    payload = value,
                )
            )

            else -> deadLetteredMessagesForKey + DeadLetteredValue(
                retryCount = 0,
                uniqueMessageId = uniqueMessageId,
                payload = value,
            )
        }.also {
            keyValueStore.put(
                /* key = */ key,
                /* value = */ it,
            )
        }
    }

    private fun reprocess(keyValue: KeyValue<K, List<DeadLetteredValue<V>>>) {
        val key = keyValue.key
        val messages = keyValue.value
        val skippedMessageLogTemplate =
            "Skipping reprocessing of message with key: '{}' and value: '{}' due to max retry attempts reached."

        when (processingMode) {
            Mode.ORDERED -> {
                val firstMessage = messages.firstOrNull()

                firstMessage?.let {
                    if (it.retryCount >= maxRetries) {
                        logger.info(skippedMessageLogTemplate, key, it)
                        return@let
                    }

                    process(
                        key = key,
                        message = firstMessage,
                        messages = messages
                    )
                }
            }
            Mode.UNORDERED -> {
                messages.forEach { message ->
                    if (message.retryCount >= maxRetries) {
                        logger.info(skippedMessageLogTemplate, key, message)
                        return@forEach
                    }

                    process(
                        key = key,
                        message = message,
                        messages = messages
                    )
                }
            }
        }
    }

    private fun process(
        key: K,
        message: DeadLetteredValue<V>,
        messages: List<DeadLetteredValue<V>>,
    ) {
        logger.info("Reprocessing dead-lettered message with key: '$key' and value: '$message'")

        try {
            businessLogic(key, message.payload)
            logger.info("Successfully reprocessed message with key: '${key}' and value: '${message}'")

            keyValueStore.put(
                /* key = */ key,
                /* value = */ messages.filter { otherMessage ->
                    otherMessage.uniqueMessageId != message.uniqueMessageId
                }
            )
        } catch (runtimeException: RuntimeException) {
            logger.error(
                "Failed to reprocess message with key: '$key' and value: '${message.payload}'",
                runtimeException
            )

            keyValueStore.put(
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
