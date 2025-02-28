plugins {
    id("java-library")
    id("pmd")
    id("checkstyle")
    id("jacoco")
    id("com.github.spotbugs")
    id("com.diffplug.spotless")
}

group = "nl.hh"
version = project.version

java {
    sourceCompatibility = JavaVersion.VERSION_23
    targetCompatibility = JavaVersion.VERSION_23
}

pmd {
    isConsoleOutput = true
    toolVersion = "7.0.0"
    threads = 1
    ruleSetFiles(rootProject.files("config/pmd/pmd.xml"))
    ruleSets = emptyList()
    incrementalAnalysis = true
}

checkstyle {
    toolVersion = "10.21.3"
}

jacoco {
    toolVersion = "0.8.12"
}

tasks.spotbugsMain {
    reports.create("html") {
        required = true
        outputLocation = layout.buildDirectory.file("reports/spotbugs.html")
        setStylesheet("fancy-hist.xsl")
    }
}

spotless {
    java {
        googleJavaFormat()
    }
}

// Workaround for conflicting dependencies
// See https://github.com/checkstyle/checkstyle/issues/14211
// NOT REQUIRED ANYMORE?

repositories {
    mavenCentral()
}

val jcipVersion = "1.0"
val junitVersion = "5.12.0"
val slf4jVersion = "2.0.17"
val spotbugsAnnotationsVersion = "4.9.1"

dependencies {
    compileOnly("net.jcip:jcip-annotations:$jcipVersion")
    compileOnly("com.github.spotbugs:spotbugs-annotations:$spotbugsAnnotationsVersion")

    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testImplementation("org.slf4j:slf4j-simple:$slf4jVersion")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1024m"
    finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)

    reports {
        xml.required = false
        csv.required = false
        html.outputLocation = layout.buildDirectory.dir("reports/jacoco")
    }
}