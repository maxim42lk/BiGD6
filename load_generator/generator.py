# load_generator/generator.py
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import time
import json
from kafka import KafkaProducer
import threading

class HospitalDataGenerator:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topics = ['hospital_admissions', 'patient_vitals', 'lab_results']
        
        # Генерация справочников
        self.first_names = ['John', 'Jane', 'Robert', 'Mary', 'Michael', 'Linda', 
                           'William', 'Elizabeth', 'David', 'Jennifer']
        self.last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 
                          'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
        self.diagnoses = ['Hypertension', 'Diabetes', 'CHD', 'COPD', 'Asthma',
                         'Pneumonia', 'Sepsis', 'Stroke', 'MI', 'Arrhythmia']
        self.medications = ['Lisinopril', 'Metformin', 'Atorvastatin', 'Aspirin',
                           'Metoprolol', 'Warfarin', 'Insulin', 'Furosemide']
        
    def generate_patient(self, patient_id):
        """Генерация данных пациента"""
        age = random.randint(18, 90)
        gender = random.choice(['Male', 'Female', 'Other'])
        
        # Генерация физиологических показателей
        systolic = random.randint(90, 180)
        diastolic = random.randint(60, 120)
        blood_pressure = f"{systolic}/{diastolic}"
        
        cholesterol = random.randint(150, 300)
        bmi = round(random.uniform(18.5, 40.0), 1)
        
        # Хронические заболевания
        diabetes = 'Yes' if random.random() < 0.25 else 'No'
        hypertension = 'Yes' if random.random() < 0.35 else 'No'
        
        # Госпитализация
        admission_date = datetime.now() - timedelta(days=random.randint(0, 30))
        length_of_stay = random.randint(1, 21)
        discharge_date = admission_date + timedelta(days=length_of_stay)
        
        # Лечение
        medication_count = random.randint(0, 10)
        
        # Назначение после выписки
        destinations = ['Home', 'Nursing_Facility', 'Rehab', 'Hospice']
        discharge_destination = random.choice(destinations)
        
        # Риск повторной госпитализации
        readmission_risk = self.calculate_readmission_risk(
            age, bmi, cholesterol, diabetes, hypertension
        )
        readmitted = 'Yes' if random.random() < readmission_risk else 'No'
        
        patient = {
            'patient_id': patient_id,
            'age': age,
            'gender': gender,
            'blood_pressure': blood_pressure,
            'cholesterol': cholesterol,
            'bmi': bmi,
            'diabetes': diabetes,
            'hypertension': hypertension,
            'medication_count': medication_count,
            'admission_date': admission_date.isoformat(),
            'discharge_date': discharge_date.isoformat(),
            'length_of_stay': length_of_stay,
            'discharge_destination': discharge_destination,
            'readmitted_30_days': readmitted,
            'timestamp': datetime.now().isoformat()
        }
        
        return patient
    
    def calculate_readmission_risk(self, age, bmi, cholesterol, diabetes, hypertension):
        """Расчет риска повторной госпитализации"""
        risk = 0.1  # Базовый риск
        
        # Факторы риска
        if age > 65:
            risk += 0.15
        if bmi > 30:
            risk += 0.1
        if cholesterol > 240:
            risk += 0.05
        if diabetes == 'Yes':
            risk += 0.2
        if hypertension == 'Yes':
            risk += 0.15
        
        # Ограничение риска
        return min(risk, 0.8)
    
    def generate_vitals(self, patient_id):
        """Генерация показателей жизнедеятельности"""
        vitals = {
            'patient_id': patient_id,
            'heart_rate': random.randint(60, 120),
            'respiratory_rate': random.randint(12, 20),
            'temperature': round(random.uniform(36.0, 39.0), 1),
            'spo2': random.randint(92, 100),
            'blood_glucose': random.randint(70, 200),
            'timestamp': datetime.now().isoformat()
        }
        return vitals
    
    def generate_lab_results(self, patient_id):
        """Генерация лабораторных результатов"""
        labs = {
            'patient_id': patient_id,
            'wbc': round(random.uniform(4.0, 12.0), 1),
            'rbc': round(random.uniform(3.5, 5.5), 2),
            'hemoglobin': round(random.uniform(12.0, 18.0), 1),
            'hematocrit': round(random.uniform(36.0, 50.0), 1),
            'platelets': random.randint(150, 450),
            'sodium': random.randint(135, 145),
            'potassium': round(random.uniform(3.5, 5.0), 1),
            'creatinine': round(random.uniform(0.6, 1.2), 2),
            'timestamp': datetime.now().isoformat()
        }
        return labs
    
    def start_generation(self, patients_per_second=10, duration_minutes=5):
        """Запуск генерации данных"""
        print(f"Начало генерации данных: {patients_per_second} пациентов/сек, {duration_minutes} минут")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        patient_id = 100000
        
        while datetime.now() < end_time:
            batch_start = time.time()
            
            for _ in range(patients_per_second):
                # Генерация данных пациента
                patient_data = self.generate_patient(patient_id)
                self.producer.send(self.topics[0], patient_data)
                
                # Генерация показателей жизнедеятельности
                vitals_data = self.generate_vitals(patient_id)
                self.producer.send(self.topics[1], vitals_data)
                
                # Генерация лабораторных результатов
                labs_data = self.generate_lab_results(patient_id)
                self.producer.send(self.topics[2], labs_data)
                
                patient_id += 1
            
            # Контроль скорости
            batch_end = time.time()
            batch_time = batch_end - batch_start
            
            if batch_time < 1:
                time.sleep(1 - batch_time)
            
            # Логирование
            if patient_id % 100 == 0:
                print(f"Сгенерировано {patient_id} пациентов")
        
        self.producer.flush()
        print(f"Генерация завершена. Всего пациентов: {patient_id}")

def main():
    generator = HospitalDataGenerator(bootstrap_servers='kafka:9092')
    
    # Запуск в отдельном потоке для контроля нагрузки
    generation_thread = threading.Thread(
        target=generator.start_generation,
        kwargs={'patients_per_second': 50, 'duration_minutes': 10}
    )
    
    generation_thread.start()
    generation_thread.join()

if __name__ == "__main__":
    main()