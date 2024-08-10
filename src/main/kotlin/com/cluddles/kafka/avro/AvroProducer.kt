package com.cluddles.kafka.avro

import avro.Fruit
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

// Can view registered schemas, e.g. http://localhost:8081/schemas/ids/1
// Can auto register schemas, but also do it manually, e.g. POST to subjects/<subject-name>/versions
// You can pull the json from the producer logs, or sort it out yourself
//   curl --header "Content-Type: application/json" -d @data/fruits-value.json http://localhost:8081/subjects/fruits-value/versions
class AvroProducer(server: String, registryUrl: String) {

    constructor(host: String, port: Int): this("$host:$port", AvroConstants.REGISTRY_URL)

    private val producer: KafkaProducer<String, Fruit> = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to KafkaAvroSerializer::class.java.name,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS to "false",
            AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION to "true",
        )
    )

    init {
        logger.info { "AvroProducer started, server: $server" }
    }

    fun produce(topic: String, fruit: Fruit) {
        logger.info { "SEND: $fruit" }
        val send = producer.send(ProducerRecord(topic, "123", fruit))
        val recordMetadata = send.get()
        logger.debug { "recordMetadata: $recordMetadata" }
    }

    companion object {
        val logger = KotlinLogging.logger {}
    }

}

fun main() {
    val producer = AvroProducer("localhost", 9092)
    producer.produce("fruits", Fruit.newBuilder().setName("melon").setColour("yellow").setNumber(24).build())
}
