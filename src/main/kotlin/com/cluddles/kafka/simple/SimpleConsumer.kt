package com.cluddles.kafka.simple

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

class SimpleConsumer(server: String, topic: String, consumerGroupId: String) {

    constructor(host: String, port: Int, topic: String, consumerGroupId: String):
            this("$host:$port", topic, consumerGroupId)

    private val consumer = KafkaConsumer<String, String> (
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ConsumerConfig.CLIENT_ID_CONFIG to "simple-consumer",
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
        )
    ).apply {
        this.subscribe(listOf(topic))
    }

    init {
        logger.info { "SimpleConsumer started, server: $server, topic: $topic, consumerGroupId: $consumerGroupId" }
    }

    fun consumeForever() {
        while (true) {
            consumeOnce(Duration.ofMillis(100L))
        }
    }

    private fun consumeOnce(pollDuration: Duration) {
        consumer.poll(pollDuration).forEach {
            logger.info { "RECV: ${it.value()}" }
        }
    }

    companion object {
        private const val OFFSET_RESET = "earliest"

        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val topic = "text"
    val consumer = SimpleConsumer("localhost", 9092, topic, "simple-consumer-${UUID.randomUUID()}")
    consumer.consumeForever()
}
