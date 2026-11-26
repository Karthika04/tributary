// The Java client library: a low-level request client plus a batching
// Producer and a polling Consumer.

plugins {
    `java-library`
    jacoco
}

dependencies {
    api(project(":protocol"))

    testImplementation(project(":broker"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
