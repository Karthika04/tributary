// The Tributary broker: a TCP server that stores topic partitions in the
// storage engine and answers produce, fetch and metadata requests.

plugins {
    `java-library`
    application
    jacoco
}

application {
    mainClass = "io.github.karthika04.tributary.broker.BrokerMain"
    applicationName = "tributary-broker"
}

dependencies {
    // `api`: BrokerConfig exposes storage's LogConfig, so anyone using the broker
    // (tests in other modules, for example) needs storage on their classpath too.
    api(project(":storage"))
    implementation(project(":protocol"))
    implementation(project(":raft"))
    implementation(project(":client")) // brokers talk to each other with the client library

    // Observability (ADR 0005). The broker's code sees only facades: Micrometer's
    // MeterRegistry and SLF4J's MDC.
    implementation(libs.micrometer.core)
    implementation(libs.slf4j.api)
    // What the application plugs in behind them: Prometheus output for /metrics,
    // Logback to write the logs (plain text, or JSON with TRIBUTARY_LOG_FORMAT=JSON),
    // and a bridge that sends the JDK's System.Logger, which all the code logs to,
    // into SLF4J.
    implementation(libs.micrometer.registry.prometheus)
    runtimeOnly(libs.logback.classic)
    runtimeOnly(libs.slf4j.jdk.platform.logging)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
