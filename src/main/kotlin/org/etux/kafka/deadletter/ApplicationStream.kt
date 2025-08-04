package org.etux.kafka.deadletter

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.etux.kafka.deadletter.serdes.MyInputDeadLetterCommandSerializer
import org.slf4j.LoggerFactory
import java.lang.Thread.sleep
import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ApplicationStream(
    private val inputTopic: String,
    private val deadLetterTopic: String,
    private val bootstrapServer: String,
) {
    enum class MessageType {
        SUCCESSFUL,
        RETRY,
        FAIL_FOREVER
    }

    val producerProperties:Properties  by lazy {
        Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    }

    val deadLetterProducerProperties:Properties by lazy {
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyInputDeadLetterCommandSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }
    }

    val consumerProperties: Properties by lazy {
        Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.GROUP_ID_CONFIG, "my-application")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

    }

    private val producer: KafkaProducer<String, String> = KafkaProducer(producerProperties)
    private val deadLetterProducer: KafkaProducer<String, DeadLetterCommand<String, String>> = KafkaProducer(deadLetterProducerProperties)
    private val consumer: KafkaConsumer<String, String> = KafkaConsumer(consumerProperties)

    fun run() {
        logger.info("Starting application.")
        Executors
            .newFixedThreadPool(3)
            .also {
                it.submit(::setProducerUp)
                it.submit(::setConsumerUp)
            }
    }

    private fun setProducerUp(): () -> Unit = {
        Runtime.getRuntime().addShutdownHook(Thread {
            producer.close()
        })

        logger.info("Starting application producer.")

        var current: Long = 0
        while(true) {
            try {
                logger.info("Sending random message.")
                producer.send(
                    ProducerRecord(
                        inputTopic,
                        "key-${(current++ % 7)}",
                        enumValues<MessageType>().random().name
                    )
                ).get(10_000, TimeUnit.MILLISECONDS)
                logger.info("Sent random message $current.")
                sleep(5000)
            } catch(t:Throwable) {
                logger.warn("Unable to produce record.", t)
                sleep(10_000)
            }
        }
    }

    private fun setConsumerUp(): () -> Unit {
        Runtime.getRuntime().addShutdownHook(Thread{
            consumer.close()
            logger.info("Closed consumer.")
        })

        logger.info("Starting application consumer.")

        consumer.subscribe(listOf(inputTopic))

        while(true) {
            try {
                consumer
                    .poll(Duration.ofMillis(5000))
                    .forEach { record ->
                        logger.info("Consuming message key: ${record.key()} value: ${record.value()}.")
                        when(MessageType.valueOf(record.value())) {
                            MessageType.SUCCESSFUL -> logger.info("Message successful.")
                            MessageType.RETRY -> logger.info("Retry successful.")
                            MessageType.FAIL_FOREVER -> {
                                logger.info("Failed.")
                                throw RetryException(record.key(), record.value())
                            }
                        }
                    }
                sleep(1000)
                logger.info("Finishing consuming records. Re-starting...")
            } catch (retryException: RetryException) {
                logger.warn("Unable to consume.", retryException)
                deadLetterProducer.send(
                    ProducerRecord(
                        deadLetterTopic,
                        retryException.key,
                        PutDeadLetterCommand(
                            deadLetter = DeadLetterRecord(
                                key = retryException.key,
                                value = retryException.value
                            )
                        )
                    )
                ).get(5000, TimeUnit.MILLISECONDS)
                logger.info("Sent error to dead letter topic.", retryException)
                sleep(10_000)
            } catch(t: Throwable) {
                logger.warn("Error while consuming record.", t)
                sleep(60_000)
            }
        }
    }

    class RetryException(val key: String, val value: String, override val cause: Throwable? = null) : Exception(cause)

    private companion object {
        private val logger = LoggerFactory.getLogger(ApplicationStream::class.java)
    }
}