package com.cluddles.kafka.simple

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

class SimpleProducer(server: String) {

    constructor(host: String, port: Int): this("$host:$port")

    private val producer: KafkaProducer<String, String> = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
        )
    )

    init {
        logger.info { "SimpleProducer started, server: $server" }
    }

    fun produce(topic: String, message: String) {
        logger.info { "SEND: $message" }
        val send = producer.send(ProducerRecord(topic, message))
        val recordMetadata = send.get()
        logger.debug { "recordMetadata: $recordMetadata" }
    }

    companion object {
        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val producer = SimpleProducer("localhost", 9092)
    while (true) {
        print("> ")
        val line = readlnOrNull()
        if (line.isNullOrEmpty()) break
        producer.produce("text", line)
    }
}
