package org.etux

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.etux.kafka.deadletter.ExampleKafkaApplication
import org.etux.kafka.deadletter.DeadLetterAdmin
import org.etux.kafka.deadletter.DeadLetterPlayer
import org.etux.kafka.deadletter.DeadLetterStream
import java.util.Properties
import java.util.concurrent.Executors

// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    val deadLetterStream =
        DeadLetterStream(
            "my-topic-dlt",
            "my-state-store",
            properties("localhost:9092"),
        )

    val deadLetterPlayer =
        DeadLetterPlayer(
            inputStore = "my-state-store",
            properties = properties("localhost:9092"),
            topology = deadLetterStream.topology,
        )

    val deadLetterAdmin =
        DeadLetterAdmin(
            properties = properties("localhost:9092"),
            topics =
                listOf(
                    "my-topic",
                    "my-topic-dlt",
                    "$APPLICATION_ID-my-state-store-changelog",
                ),
        )

    deadLetterAdmin.run()

    val applicationStream =
        ExampleKafkaApplication(
            inputTopic = "my-topic",
            deadLetterTopic = "my-topic-dlt",
            bootstrapServer = "localhost:9092",
            store = deadLetterPlayer.store,
        )

    Executors
        .newScheduledThreadPool(3)
        .also {
            it.submit(deadLetterStream::run)
            it.submit(deadLetterPlayer::run)
            it.submit(applicationStream::run)
        }
}

private const val APPLICATION_ID = "dead-letter"

fun properties(bootstrapServers: String) =
    Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID)
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    }
