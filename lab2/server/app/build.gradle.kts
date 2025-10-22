plugins {
    application
    id("com.gradleup.shadow") version "9.2.2"
}

repositories {
    mavenCentral()
}

dependencies {
    // оставлю как в твоём шаблоне — при желании удали guava
    implementation(libs.guava)
    testImplementation(libs.junit)
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("org.example.Server")
}

tasks.jar {
    // чтобы обычный jar тоже был исполняемым (а shadowJar это унаследует)
    manifest {
        attributes("Main-Class" to "org.example.Server")
    }
}

tasks.shadowJar {
    // делаем «красивое» имя файла fat-jar
    archiveBaseName.set("tcp-server-fat")
    archiveVersion.set("")
    archiveClassifier.set("")
}

// опционально: чтобы `./gradlew build` также собирал fat-jar
tasks.build {
    dependsOn(tasks.shadowJar)
}
