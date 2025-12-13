# Dockerfile
FROM ubuntu:22.04

# Установка Java 11 и зависимостей
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk-headless \
    wget \
    curl \
    python3 \
    python3-pip \
    libsnappy1v5 \
    libgomp1 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Установка Apache Spark
ARG SPARK_VERSION=3.3.0
ARG HADOOP_VERSION=3.3.2

# Используем явные переменные для избежания проблем с подстановкой
RUN SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_FILE} && \
    tar xzf ${SPARK_FILE} -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm ${SPARK_FILE}

# Установка Python зависимостей
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Настройка переменных окружения
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Оптимизация для контейнеров
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

# Создание рабочей директории
WORKDIR /app

# Копирование исходного кода и данных
COPY src/ /app/src/
COPY data/ /app/data/
COPY tests/ /app/tests/

# Экспорт портов
EXPOSE 8080 7077 4040

# Команда по умолчанию
CMD ["/bin/bash"]