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

    testImplementation(project(":client"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
