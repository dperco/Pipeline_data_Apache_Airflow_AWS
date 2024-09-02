# Pipeline_data_Apache_Airflow_AWS
Pipeline injesta de datos , ETL ,monitoreo y almacenamiento  utilizando Airflow-Kafcke,Spark , Python, Postgress, SQL,AWS 
# Pipeline de Monitoreo y Análisis de Datos

Descripción
Este proyecto implementa un pipeline de monitoreo y análisis de datos utilizando Apache Airflow, Kafka, Kubernetes, Spark, PostgreSQL y AWS. El pipeline se encarga de procesar datos, monitorear métricas y logs, enviar alertas y generar reportes. Además, se realizan cálculos específicos para medir el Net Promoter Score (NPS) en diferentes categorías y equipos.

Herramientas Utilizadas
Apache Airflow: Orquestación de flujos de trabajo.
Kafka: Plataforma de streaming de datos.
Kubernetes: Gestión de contenedores y orquestación.
Spark: Procesamiento de datos a gran escala.
PostgreSQL: Base de datos relacional.
AWS: Infraestructura en la nube.
Prometheus: Monitoreo y alertas.
Python: Lenguaje de programación principal.
pandas: Biblioteca para manipulación y análisis de datos.
prometheus_client: Biblioteca para integrar Prometheus en Python.
Estructura del Pipeline
Inicialización
Tarea Principal: inicializar_task
Función: Configura el entorno y prepara los recursos necesarios para las siguientes etapas.
Conexión a Kafka
Tarea Principal: conexion_kafka_task
Función: Conecta a Kafka y asegura que los datos se consuman correctamente desde los tópicos configurados.
Tareas de Kubernetes
Tarea Principal: kubernetes_task
Función: Ejecuta scripts y comandos necesarios para la gestión de recursos en Kubernetes.
Conexión a Spark
Tarea Principal: conexion_spark_task
Función: Configura y conecta a un clúster de Spark para el procesamiento de datos.
Transformación de Datos
Tarea Principal: transformar_datos_task
Función: Aplica transformaciones y limpiezas a los datos para prepararlos para su carga final.
Carga de Datos
Tarea Principal: cargar_datos_task
Función: Transfiere los datos procesados y transformados al destino final.
Monitoreo
Funciones de Monitoreo
Monitor Logs: Monitorea los logs del sistema.
Monitor Metrics: Monitorea las métricas de rendimiento utilizando Prometheus.
Send Alerts: Envía alertas si se detectan problemas.
Generate Reports: Genera reportes basados en los datos de monitoreo.
Código de Monitoreo
python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import smtplib
from email.mime.text import MIMEText
import pandas as pd
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Funciones de monitoreo

def monitor_logs():
    logging.info("Monitoreando logs...")
    logs = "Ejemplo de log: Error en la conexión a la base de datos."
    if "Error" in logs:
        logging.error("Se encontró un error en los logs.")

def monitor_metrics():
    logging.info("Monitoreando métricas...")
    registry = CollectorRegistry()
    cpu_usage_gauge = Gauge('cpu_usage', 'Uso de CPU', registry=registry)
    memory_usage_gauge = Gauge('memory_usage', 'Uso de Memoria', registry=registry)
    metrics = {"cpu_usage": 75, "memory_usage": 60}
    cpu_usage_gauge.set(metrics["cpu_usage"])
    memory_usage_gauge.set(metrics["memory_usage"])
    push_to_gateway('localhost:9091', job='monitoring_job', registry=registry)
    if metrics["cpu_usage"] > 70:
        logging.warning("El uso de CPU es alto: %s%%", metrics["cpu_usage"])

def send_alerts():
    logging.info("Enviando alertas...")
    msg = MIMEText("Se ha detectado un problema en el sistema.")
    msg["Subject"] = "Alerta del sistema"

    msg["From"] = "alertas@example.com"
    msg["To"] = "admin@tuempresa.com"
    try:
        with smtplib.SMTP("localhost") as server:
            server.sendmail(msg["From"], [msg["To"]], msg.as_string())
        logging.info("Alerta enviada exitosamente.")
    except Exception as e:
        logging.error("Error al enviar la alerta: %s", e)

def generate_reports():
    logging.info("Generando reportes...")
    data = {"metric": ["cpu_usage", "memory_usage"], "value": [75, 60]}
    df = pd.DataFrame(data)
    report = df.to_csv(index=False)
    with open("/path/to/report.csv", "w") as f:
        f.write(report)
    logging.info("Reporte generado y guardado en /path/to/report.csv")

# Definición del DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitoring_dag',
    default_args=default_args,
    description='DAG de Monitoreo',
    schedule_interval=timedelta(days=1),
)

# Definición de las tareas

t1 = PythonOperator(
    task_id='monitor_logs',
    python_callable=monitor_logs,
    dag=dag,
)

t2 = PythonOperator(
    task_id='monitor_metrics',
    python_callable=monitor_metrics,
    dag=dag,
)

t3 = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts,
    dag=dag,
)

t4 = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

# Definición de las dependencias

t1 >> t2 >> t3 >> t4


Consultas SQL
Calcular el NPS para dos categorías: casos por mes que fueron derivados al menos una vez y casos que no tuvieron ninguna derivación
sql
WITH derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE interaction_type = 'rep_derivation'
),
non_derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE case_id NOT IN (SELECT case_id FROM derived_cases)
),
nps_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN derived_cases USING(case_id)
    GROUP BY month
),
nps_non_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN non_derived_cases USING(case_id)
    GROUP BY month
)
SELECT 
    month,
    'derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_derived
UNION ALL
SELECT 
    month,
    'non_derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_non_derived
ORDER BY month, category;
Visualizar el NPS por cada equipo de representantes y por mes
sql
WITH last_interaction AS (
    SELECT 
        case_id,
        MAX(int_date) AS last_date
    FROM Interacciones
    GROUP BY case_id
),
case_representatives AS (
    SELECT 
        i.case_id,
        i.representante,
        r.team
    FROM Interacciones i
    JOIN last_interaction li ON i.case_id = li.case_id AND i.int_date = li.last_date
    JOIN Representantes r ON i.representante = r.representante
),
nps_by_team AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        team,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN case_representatives USING(case_id)
    GROUP BY month, team
)
SELECT 
    month,
    team,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_by_team
ORDER BY month, team;
Notificaciones por Correo
Cada etapa del pipeline tiene una notificación por correo configurada para alertar en caso de falla. Estas notificaciones aseguran que cualquier problema en el pipeline sea detectado y gestionado rápidamente.

####################################
####################################

CON  AWS   

Para implementar el mismo pipeline utilizando herramientas de AWS, puedes aprovechar varios servicios de AWS como Amazon Managed Workflows for Apache Airflow (MWAA), Amazon MSK (Managed Streaming for Kafka), Amazon EKS (Elastic Kubernetes Service), Amazon EMR (Elastic MapReduce) para Spark, Amazon RDS para PostgreSQL, y Amazon CloudWatch para monitoreo. Aquí tienes una guía sobre cómo podrías configurar este pipeline en AWS:

Herramientas de AWS Utilizadas
Amazon MWAA: Orquestación de flujos de trabajo con Apache Airflow.
Amazon MSK: Plataforma de streaming de datos con Kafka.
Amazon EKS: Gestión de contenedores y orquestación con Kubernetes.
Amazon EMR: Procesamiento de datos a gran escala con Spark.
Amazon RDS: Base de datos relacional con PostgreSQL.
Amazon CloudWatch: Monitoreo y alertas.
AWS Lambda: Funciones serverless para tareas específicas.
Amazon SNS: Servicio de notificaciones.
Estructura del Pipeline en AWS
Inicialización
Tarea Principal: inicializar_task
Función: Configura el entorno y prepara los recursos necesarios para las siguientes etapas.
Conexión a Kafka
Tarea Principal: conexion_kafka_task
Función: Conecta a Kafka y asegura que los datos se consuman correctamente desde los tópicos configurados.
Tareas de Kubernetes
Tarea Principal: kubernetes_task
Función: Ejecuta scripts y comandos necesarios para la gestión de recursos en Kubernetes.
Conexión a Spark
Tarea Principal: conexion_spark_task
Función: Configura y conecta a un clúster de Spark para el procesamiento de datos.
Transformación de Datos
Tarea Principal: transformar_datos_task
Función: Aplica transformaciones y limpiezas a los datos para prepararlos para su carga final.
Carga de Datos
Tarea Principal: cargar_datos_task
Función: Transfiere los datos procesados y transformados al destino final.
Monitoreo
Funciones de Monitoreo
Monitor Logs: Monitorea los logs del sistema con Amazon CloudWatch.
Monitor Metrics: Monitorea las métricas de rendimiento utilizando Amazon CloudWatch.
Send Alerts: Envía alertas si se detectan problemas utilizando Amazon SNS.
Generate Reports: Genera reportes basados en los datos de monitoreo y los guarda en Amazon S3.
Código de Monitoreo
python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import boto3
import pandas as pd

# Configuración de AWS

cloudwatch = boto3.client('cloudwatch')
sns = boto3.client('sns')
s3 = boto3.client('s3')

# Funciones de las tareas de monitoreo

def monitor_logs():
    logging.info("Monitoreando logs...")
    logs = "Ejemplo de log: Error en la conexión a la base de datos."
    if "Error" in logs:
        logging.error("Se encontró un error en los logs.")
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789012:my-topic',
            Message='Se encontró un error en los logs: Error en la conexión a la base de datos.',
            Subject='Alerta de Logs'
        )

def monitor_metrics():
    logging.info("Monitoreando métricas...")
    metrics = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'cpu_usage',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/EC2',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                'Name': 'InstanceId',
                                'Value': 'i-1234567890abcdef0'
                            },
                        ]
                    },
                    'Period': 300,
                    'Stat': 'Average',
                },
                'ReturnData': True,
            },
        ],
        StartTime=datetime.utcnow() - timedelta(minutes=10),
        EndTime=datetime.utcnow(),
    )
    cpu_usage = metrics['MetricDataResults'][0]['Values'][0]
    if cpu_usage > 70:
        logging.warning("El uso de CPU es alto: %s%%", cpu_usage)
        sns.publish(
            TopicArn='arn:aws:sns:us-east-1:123456789012:my-topic',
            Message=f'El uso de CPU es alto: {cpu_usage}%',
            Subject='Alerta de Métricas'
        )

def send_alerts():
    logging.info("Enviando alertas...")
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:123456789012:my-topic',
        Message='Se ha detectado un problema en el sistema.',
        Subject='Alerta del Sistema'
    )

def generate_reports():
    logging.info("Generando reportes...")
    data = {"metric": ["cpu_usage", "memory_usage"], "value": [75, 60]}
    df = pd.DataFrame(data)
    report = df.to_csv(index=False)
    s3.put_object(Bucket='my-bucket', Key='report.csv', Body=report)
    logging.info("Reporte generado y guardado en S3")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monitoring_dag',
    default_args=default_args,
    description='DAG de Monitoreo',
    schedule_interval=timedelta(days=1),
)

# Definición de las tareas
t1 = PythonOperator(
    task_id='monitor_logs',
    python_callable=monitor_logs,
    dag=dag,
)

t2 = PythonOperator(
    task_id='monitor_metrics',
    python_callable=monitor_metrics,
    dag=dag,
)

t3 = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts,
    dag=dag,
)

t4 = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

# Definición de las dependencias
t1 >> t2 >> t3 >> t4
Consultas SQL
Calcular el NPS para dos categorías: casos por mes que fueron derivados al menos una vez y casos que no tuvieron ninguna derivación
sql
WITH derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE interaction_type = 'rep_derivation'
),
non_derived_cases AS (
    SELECT DISTINCT case_id
    FROM Interacciones
    WHERE case_id NOT IN (SELECT case_id FROM derived_cases)
),
nps_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN derived_cases USING(case_id)
    GROUP BY month
),
nps_non_derived AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN non_derived_cases USING(case_id)
    GROUP BY month
)
SELECT 
    month,
    'derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_derived
UNION ALL
SELECT 
    month,
    'non_derived' AS category,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_non_derived
ORDER BY month, category;
Visualizar el NPS por cada equipo de representantes y por mes
sql
WITH last_interaction AS (
    SELECT 
        case_id,
        MAX(int_date) AS last_date
    FROM Interacciones
    GROUP BY case_id
),
case_representatives AS (
    SELECT 
        i.case_id,
        i.representante,
        r.team
    FROM Interacciones i
    JOIN last_interaction li ON i.case_id = li.case_id AND i.int_date = li.last_date
    JOIN Representantes r ON i.representante = r.representante
),
nps_by_team AS (
    SELECT 
        DATE_TRUNC('month', int_date) AS month,
        team,
        SUM(CASE WHEN nps_score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN nps_score BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detractors,
        COUNT(*) AS total_surveys
    FROM NPS
    JOIN case_representatives USING(case_id)
    GROUP BY month, team
)
SELECT 
    month,
    team,
    (promoters - detractors) * 100.0 / total_surveys AS nps
FROM nps_by_team
ORDER BY month, team;
Notificaciones por Correo
Cada etapa del pipeline tiene una notificación por correo configurada para alertar en caso de falla. Estas notificaciones aseguran que cualquier problema en el pipeline sea detectado y gestionado rápidamente.


