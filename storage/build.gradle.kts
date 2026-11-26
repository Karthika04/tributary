// The storage module: Tributary's append-only, segmented commit log.
// It depends only on the JDK, so every other module can build on it.

plugins {
    `java-library`
    jacoco
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    reports {
        xml.required = true
        html.required = true
    }
}
