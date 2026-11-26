rootProject.name = "tributary"

// Where Gradle downloads third-party libraries (JUnit, ...) from.
dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("storage", "protocol", "broker", "client")
