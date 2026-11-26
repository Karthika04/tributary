// Shared build settings for every module in Tributary.
// Each module (storage, broker, client, ...) has its own build.gradle.kts
// for its dependencies; anything common to all of them lives here.

allprojects {
    group = "io.github.karthika04.tributary"
    version = "0.1.0-SNAPSHOT"
}

subprojects {
    tasks.withType<JavaCompile>().configureEach {
        // Compile for Java 21 bytecode. Any JDK 21+ (including your JDK 25)
        // can build and run it, and CI can use the newest JDK.
        options.release = 21
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-serial", "-Xlint:-processing"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Print the full reason when a test fails, instead of a one-line summary.
        testLogging {
            events("failed", "skipped")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            showStandardStreams = false
        }
    }
}
