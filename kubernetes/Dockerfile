FROM ubuntu:22.04

# Установка Java 11 (требуется для Spark 3.x)
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless

# Установка native-зависимостей
RUN apt-get install -y libsnappy1v5 libgomp1 zlib1g

# Скачивание Spark
ENV SPARK_VERSION=3.3.0
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz
RUN tar xzf spark-${SPARK_VERSION}-bin-hadoop3.2.tgz -C /opt/
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3.2 /opt/spark

# Настройка переменных окружения
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Оптимизация для контейнеров
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"