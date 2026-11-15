# syntax=docker/dockerfile:1

# ---- build: compile the job and collect the JDBC driver ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY .mvn .mvn
RUN mvn -B -q dependency:go-offline
COPY src src
RUN mvn -B -q package -DskipTests

# ---- run: spark-submit in the official Spark image ----
FROM apache/spark:3.5.3-java17
COPY --from=build /src/target/book-recommender-als-*.jar /opt/app/app.jar
COPY --from=build /src/target/lib/ /opt/app/lib/
ENV SPARK_MASTER=local[*]
ENTRYPOINT ["/bin/sh", "-c", "exec /opt/spark/bin/spark-submit --master \"$SPARK_MASTER\" --jars \"$(ls /opt/app/lib/*.jar | paste -sd, -)\" --class io.github.muhammedts.recommender.RecommenderJob /opt/app/app.jar"]
