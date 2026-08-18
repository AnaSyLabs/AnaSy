FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY event-connector-starter event-connector-starter
COPY examples examples
COPY sinks sinks
ARG MODULE
RUN mvn -pl ${MODULE} -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
ARG MODULE
COPY --from=build /src/${MODULE}/target/*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
