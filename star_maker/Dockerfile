FROM flink:1.20.3

RUN apt-get update && apt-get install -y maven

COPY pom.xml .
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/flink/lib
