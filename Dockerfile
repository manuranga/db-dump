FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -q

FROM eclipse-temurin:21-jre
COPY --from=build /src/target/db-dump.jar /app/db-dump.jar
WORKDIR /app
CMD ["java", "-jar", "/app/db-dump.jar"]
