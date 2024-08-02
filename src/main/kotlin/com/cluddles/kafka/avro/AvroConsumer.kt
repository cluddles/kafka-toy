package com.cluddles.kafka.avro

import avro.Fruit
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

class AvroConsumer(server: String, topic: String, consumerGroupId: String, registryUrl: String) {

    constructor(host: String, port: Int, topic: String, consumerGroupId: String):
            this("$host:$port", topic, consumerGroupId, AvroConstants.REGISTRY_URL)

    private val consumer = KafkaConsumer<String, Fruit> (
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ConsumerConfig.CLIENT_ID_CONFIG to "avro-consumer",
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to OFFSET_RESET,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to KafkaAvroDeserializer::class.java.name,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to "false",
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION to "true",
        )
    ).apply { this.subscribe(listOf(topic)) }

    init {
        logger.info { "AvroConsumer started, server: $server, topic: $topic, consumerGroupId: $consumerGroupId" }
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
    val consumer = AvroConsumer("localhost", 9092, "fruits", "avro-consumer-${UUID.randomUUID()}")
    consumer.consumeForever()
}
