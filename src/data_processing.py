# src/data_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, stddev, round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
import pandas as pd
import mlflow
import mlflow.spark
import time
import os

class HospitalDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("HospitalReadmissionsAnalysis") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "100") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "2") \
            .config("spark.dynamicAllocation.maxExecutors", "10") \
            .getOrCreate()
        
        self.schema = StructType([
            StructField("patient_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("blood_pressure", StringType(), True),
            StructField("cholesterol", IntegerType(), True),
            StructField("bmi", DoubleType(), True),
            StructField("diabetes", StringType(), True),
            StructField("hypertension", StringType(), True),
            StructField("medication_count", IntegerType(), True),
            StructField("length_of_stay", IntegerType(), True),
            StructField("discharge_destination", StringType(), True),
            StructField("readmitted_30_days", StringType(), True)
        ])
        
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.set_experiment("hospital_readmissions")
    
    def load_data(self, file_path):
        """Загрузка данных из CSV файла"""
        start_time = time.time()
        
        # Чтение данных
        df = self.spark.read \
            .option("header", "true") \
            .schema(self.schema) \
            .csv(file_path)
        
        load_time = time.time() - start_time
        
        # Логирование метрик
        with mlflow.start_run(run_name="data_loading"):
            mlflow.log_metric("load_time_seconds", load_time)
            mlflow.log_metric("row_count", df.count())
            mlflow.log_metric("column_count", len(df.columns))
            
            # Сохранение артефактов
            pandas_df = df.limit(1000).toPandas()
            pandas_df.to_csv("/tmp/sample_data.csv", index=False)
            mlflow.log_artifact("/tmp/sample_data.csv")
        
        return df
    
    def preprocess_data(self, df):
        """Предобработка данных"""
        start_time = time.time()
        
        # Извлечение systolic и diastolic давления
        df = df.withColumn("systolic_bp", 
                          col("blood_pressure").substr(1, 3).cast("int"))
        df = df.withColumn("diastolic_bp", 
                          col("blood_pressure").substr(5, 2).cast("int"))
        
        # Кодирование категориальных переменных
        df = df.withColumn("diabetes_encoded", 
                          when(col("diabetes") == "Yes", 1).otherwise(0))
        df = df.withColumn("hypertension_encoded", 
                          when(col("hypertension") == "Yes", 1).otherwise(0))
        df = df.withColumn("readmitted_encoded", 
                          when(col("readmitted_30_days") == "Yes", 1).otherwise(0))
        
        # Кодирование discharge_destination
        from pyspark.ml.feature import StringIndexer
        indexer = StringIndexer(inputCol="discharge_destination", 
                               outputCol="destination_encoded")
        df = indexer.fit(df).transform(df)
        
        # Создание фич
        df = df.withColumn("age_group", 
                          when(col("age") < 30, "young")
                          .when((col("age") >= 30) & (col("age") < 60), "middle")
                          .otherwise("senior"))
        
        df = df.withColumn("bmi_category", 
                          when(col("bmi") < 18.5, "underweight")
                          .when((col("bmi") >= 18.5) & (col("bmi") < 25), "normal")
                          .when((col("bmi") >= 25) & (col("bmi") < 30), "overweight")
                          .otherwise("obese"))
        
        # Логирование метрик предобработки
        processing_time = time.time() - start_time
        
        with mlflow.start_run(run_name="data_preprocessing"):
            mlflow.log_metric("processing_time_seconds", processing_time)
            mlflow.log_metric("features_created", 6)
            
            # Статистики по данным
            stats_df = df.describe(["age", "cholesterol", "bmi", "length_of_stay"])
            stats_pandas = stats_df.toPandas()
            stats_pandas.to_csv("/tmp/data_statistics.csv", index=False)
            mlflow.log_artifact("/tmp/data_statistics.csv")
        
        return df
    
    def analyze_readmissions(self, df):
        """Анализ повторных госпитализаций"""
        start_time = time.time()
        
        # Статистики по повторным госпитализациям
        readmission_stats = df.groupBy("readmitted_30_days").agg(
            count("*").alias("patient_count"),
            avg("age").alias("avg_age"),
            avg("length_of_stay").alias("avg_los"),
            avg("medication_count").alias("avg_medications")
        )
        
        # Анализ по возрастным группам
        age_analysis = df.groupBy("age_group", "readmitted_30_days").agg(
            count("*").alias("count")
        )
        
        # Анализ по хроническим заболеваниям
        chronic_analysis = df.groupBy("diabetes", "hypertension", "readmitted_30_days").agg(
            count("*").alias("patient_count"),
            avg("length_of_stay").alias("avg_los")
        )
        
        # Расчет стоимости
        cost_analysis = self.calculate_costs(df)
        
        analysis_time = time.time() - start_time
        
        # Логирование метрик анализа
        with mlflow.start_run(run_name="readmission_analysis"):
            mlflow.log_metric("analysis_time_seconds", analysis_time)
            mlflow.log_metric("readmission_rate", 
                            df.filter(col("readmitted_30_days") == "Yes").count() / df.count())
            mlflow.log_metric("total_patients", df.count())
            
            # Сохранение результатов анализа
            readmission_stats_pandas = readmission_stats.toPandas()
            readmission_stats_pandas.to_csv("/tmp/readmission_stats.csv", index=False)
            mlflow.log_artifact("/tmp/readmission_stats.csv")
            
            age_analysis_pandas = age_analysis.toPandas()
            age_analysis_pandas.to_csv("/tmp/age_analysis.csv", index=False)
            mlflow.log_artifact("/tmp/age_analysis.csv")
        
        return {
            "readmission_stats": readmission_stats,
            "age_analysis": age_analysis,
            "chronic_analysis": chronic_analysis,
            "cost_analysis": cost_analysis
        }
    
    def calculate_costs(self, df):
        """Расчет стоимости госпитализаций"""
        # Средняя стоимость дня госпитализации
        COST_PER_DAY = 1500  # USD
        
        total_cost = df.agg(
            (sum(col("length_of_stay")) * COST_PER_DAY).alias("total_cost")
        ).collect()[0]["total_cost"]
        
        readmission_cost = df.filter(col("readmitted_30_days") == "Yes").agg(
            (sum(col("length_of_stay")) * COST_PER_DAY).alias("readmission_cost")
        ).collect()[0]["readmission_cost"]
        
        return {
            "total_cost": total_cost,
            "readmission_cost": readmission_cost,
            "cost_savings_potential": readmission_cost * 0.3  # Потенциал экономии 30%
        }
    
    def train_prediction_model(self, df):
        """Обучение модели прогнозирования повторных госпитализаций"""
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        
        start_time = time.time()
        
        # Подготовка фич
        feature_cols = ["age", "cholesterol", "bmi", "medication_count", 
                       "length_of_stay", "diabetes_encoded", "hypertension_encoded"]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Разделение данных
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Создание и обучение модели
        lr = LogisticRegression(featuresCol="features", labelCol="readmitted_encoded",
                               maxIter=100, regParam=0.01, elasticNetParam=0.8)
        
        pipeline = Pipeline(stages=[assembler, lr])
        model = pipeline.fit(train_df)
        
        # Оценка модели
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol="readmitted_encoded")
        auc = evaluator.evaluate(predictions)
        
        training_time = time.time() - start_time
        
        # Логирование модели в MLflow
        with mlflow.start_run(run_name="logistic_regression_model"):
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("training_time_seconds", training_time)
            mlflow.log_param("maxIter", 100)
            mlflow.log_param("regParam", 0.01)
            
            # Сохранение модели
            mlflow.spark.log_model(model, "logistic_regression_model")
            
            # Логирование feature importance
            importance = model.stages[-1].coefficients.toArray()
            feature_importance = dict(zip(feature_cols, importance))
            import json
            with open("/tmp/feature_importance.json", "w") as f:
                json.dump(feature_importance, f)
            mlflow.log_artifact("/tmp/feature_importance.json")
        
        return model, auc
    
    def run_pipeline(self, data_path):
        """Запуск полного пайплайна"""
        print("Шаг 1: Загрузка данных...")
        df = self.load_data(data_path)
        
        print("Шаг 2: Предобработка данных...")
        df = self.preprocess_data(df)
        
        print("Шаг 3: Анализ повторных госпитализаций...")
        analysis_results = self.analyze_readmissions(df)
        
        print("Шаг 4: Обучение модели прогнозирования...")
        model, auc = self.train_prediction_model(df)
        
        print("Шаг 5: Расчет стоимости и оптимизации...")
        cost_results = analysis_results["cost_analysis"]
        
        # Вывод результатов
        print(f"\nРезультаты анализа:")
        print(f"Всего пациентов: {df.count()}")
        print(f"Коэффициент повторных госпитализаций: {df.filter(col('readmitted_30_days') == 'Yes').count() / df.count():.2%}")
        print(f"AUC модели: {auc:.4f}")
        print(f"Общая стоимость госпитализаций: ${cost_results['total_cost']:,.2f}")
        print(f"Стоимость повторных госпитализаций: ${cost_results['readmission_cost']:,.2f}")
        print(f"Потенциальная экономия: ${cost_results['cost_savings_potential']:,.2f}")
        
        return {
            "dataframe": df,
            "model": model,
            "auc": auc,
            "cost_analysis": cost_results
        }

def main():
    processor = HospitalDataProcessor()
    data_path = "/app/data/hospital_readmissions_30k.csv"
    
    results = processor.run_pipeline(data_path)
    
    # Сохранение результатов
    results_df = results["dataframe"]
    results_df.write \
        .mode("overwrite") \
        .parquet("/tmp/hospital_data_processed.parquet")
    
    print("\nПайплайн успешно выполнен!")
    print(f"Обработано записей: {results_df.count()}")
    print(f"Сохранено в: /tmp/hospital_data_processed.parquet")

if __name__ == "__main__":
    main()