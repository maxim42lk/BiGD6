# load_generator/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY generator.py .

# Установка Kafka и других зависимостей
RUN pip install kafka-python pandas numpy

CMD ["python", "generator.py"]