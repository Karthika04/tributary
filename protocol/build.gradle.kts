// The wire protocol shared by the broker and its clients: message formats,
// framing, error codes, compression and key-based partitioning.

plugins {
    `java-library`
    jacoco
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
