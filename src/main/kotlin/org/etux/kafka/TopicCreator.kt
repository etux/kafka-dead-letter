package org.etux.kafka

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.Properties

class TopicCreator(
    private val bootstrapServer: String,
    private val topics: List<String>,
) {
    fun run() {
            try {
                Admin.create(
                    Properties().apply {
                        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
                    }
                ).use { admin ->
                    topics
                        .map { topicName -> NewTopic(
                            /* name = */ topicName,
                            /* numPartitions = */ 2,
                            /* replicationFactor = */ 1
                        ) }
                        .map { newTopic ->
                            try {
                                admin.createTopics(listOf(newTopic)).all().get()
                            } catch(t: Throwable) {
                                logger.warn("Unable to create topic ${newTopic.name()} due to ", t)
                            }
                        }.forEach { result ->
                            if (result != null) {
                                logger.info("Created topic.")
                            }
                        }
                }
            } catch(t: Throwable) {
                logger.warn("An error occurred while creating the topics.", t)
            }
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(TopicCreator::class.java)
    }
}