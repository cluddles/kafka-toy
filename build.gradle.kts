group = "com.cluddles"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation(libs.kotlin.logging)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j2)
    implementation(libs.kafka.clients)
    implementation(libs.kafka.avro.serializer)

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

buildscript {
    repositories {
        gradlePluginPortal()
        maven("https://packages.confluent.io/maven/")
        maven("https://jitpack.io")
    }
}

plugins {
    kotlin("jvm") version "2.0.0"
    alias(libs.plugins.avro)
    alias(libs.plugins.schema.registry)
}

tasks.register<JavaExec>("simpleConsumer") {
    description = "Run the SimpleConsumer"
    group = "run"
    mainClass.set("com.cluddles.kafka.SimpleConsumerKt")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("simpleProducer") {
    description = "Run the SimpleProducer"
    group = "run"
    mainClass.set("com.cluddles.kafka.SimpleProducerKt")
    classpath = sourceSets["main"].runtimeClasspath
}

schemaRegistry {
    url = "http://localhost:8081"
    register {
        subject("fruits-value", "src/main/avro/Fruit.avsc", "AVRO").setNormalized(true)
    }
}
