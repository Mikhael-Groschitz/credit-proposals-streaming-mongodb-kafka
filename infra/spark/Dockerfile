ARG SPARK_VERSION=3.5.8
ARG SPARK_IMAGE_TAG=3.5.8-scala2.12-java11-python3-ubuntu

FROM spark:${SPARK_IMAGE_TAG} AS resolver

ARG SPARK_VERSION
ARG DELTA_VERSION=3.3.2
ARG SCALA_VERSION=2.12

USER root

RUN "$SPARK_HOME/bin/spark-submit" \
      --master "local[1]" \
      --packages "org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION},io.delta:delta-spark_${SCALA_VERSION}:${DELTA_VERSION}" \
      --conf spark.jars.ivy=/tmp/ivy \
      --class org.apache.spark.examples.SparkPi \
      "$SPARK_HOME/examples/jars/spark-examples_${SCALA_VERSION}-${SPARK_VERSION}.jar" 10

FROM spark:${SPARK_IMAGE_TAG}

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=resolver /tmp/ivy/jars/*.jar "$SPARK_HOME/jars/"

RUN curl -fsSL -o "$SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

COPY conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

USER spark
