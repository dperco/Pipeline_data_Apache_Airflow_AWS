from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import smtplib
from email.mime.text import MIMEText
import pandas as pd
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Funciones de las tareas de monitoreo
def monitor_logs():
    # Ejemplo de lógica para monitorear logs
    logging.info("Monitoreando logs...")
    # Aquí podrías agregar la lógica para leer logs desde un archivo o una fuente externa
    logs = "Ejemplo de log: Error en la conexión a la base de datos."
    if "Error" in logs:
        logging.error("Se encontró un error en los logs.")

def monitor_metrics():
    # Ejemplo de lógica para monitorear métricas
    logging.info("Monitoreando métricas...")
    # Configurar Prometheus
    registry = CollectorRegistry()
    cpu_usage_gauge = Gauge('cpu_usage', 'Uso de CPU', registry=registry)
    memory_usage_gauge = Gauge('memory_usage', 'Uso de Memoria', registry=registry)

    # Obtener métricas desde una fuente externa como Prometheus
    metrics = {"cpu_usage": 75, "memory_usage": 60}
    cpu_usage_gauge.set(metrics["cpu_usage"])
    memory_usage_gauge.set(metrics["memory_usage"])

    # Enviar métricas a Prometheus Pushgateway
    push_to_gateway('localhost:9091', job='monitoring_job', registry=registry)

    if metrics["cpu_usage"] > 70:
        logging.warning("El uso de CPU es alto: %s%%", metrics["cpu_usage"])

def send_alerts():
    # Ejemplo de lógica para enviar alertas
    logging.info("Enviando alertas...")
    # Aquí podrías agregar la lógica para enviar alertas por correo electrónico o a un sistema de mensajería
    msg = MIMEText("Se ha detectado un problema en el sistema.")
    msg["Subject"] = "Alerta del sistema"
    msg["From"] = "alertas@tuempresa.com"
    msg["To"] = "admin@tuempresa.com"

    try:
        with smtplib.SMTP("localhost") as server:
            server.sendmail(msg["From"], [msg["To"]], msg.as_string())
        logging.info("Alerta enviada exitosamente.")
    except Exception as e:
        logging.error("Error al enviar la alerta: %s", e)

def generate_reports():
    # Ejemplo de lógica para generar reportes
    logging.info("Generando reportes...")
    # Aquí podrías agregar la lógica para generar reportes a partir de datos almacenados
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