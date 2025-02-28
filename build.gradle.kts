plugins {
    id("java-library")
    id("pmd")
    id("checkstyle")
    id("jacoco")
    id("org.sonarqube") version "6.0.1.5171"
    id("com.github.spotbugs") version "6.1.6" apply false
    id("com.diffplug.spotless") version "7.0.2" apply false
}

group = "nl.hh"
version = "0.0.1-SNAPSHOT"

sonarqube {
    properties {
        property("sonar.gradle.skipCompile", "true")
    }
}

sonar {
    properties {
        property("sonar.gradle.skipCompile", "true")
        property("sonar.projectKey", "houthacker_luxor-db")
        property("sonar.organization", "houthacker")
        property("sonar.host.url", "https://sonarcloud.io")
    }
}