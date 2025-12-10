import org.gradle.api.file.DuplicatesStrategy

plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("dnsjava:dnsjava:3.5.2")
}

application {
    mainClass.set("org.example.socks5.Main")
}

tasks.test {
    enabled = false
}

// Делаем жирный jar c манифестом и всеми зависимостями
tasks.jar {
    manifest {
        attributes["Main-Class"] = "org.example.socks5.Main"
    }

    // Кладём внутрь все runtime-зависимости
    from(
        configurations.runtimeClasspath.get().map { file ->
            if (file.isDirectory) file else zipTree(file)
        }
    )

    // На случай дублирующихся файлов (META-INF и пр.)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
