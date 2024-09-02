from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['correo_aviso_fallas@example.com'],
}

dag = DAG(
    'pipeline_general',
    default_args=default_args,
    schedule_interval='@daily',
)

# Inicialización
inicializar_task = DummyOperator(
    task_id='inicializar',
    dag=dag,
)

email_inicializacion_task = EmailOperator(
    task_id='enviar_email_falla_inicializacion',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Inicialización',
    html_content='Ha ocurrido una falla en el DAG de Inicialización.',
    trigger_rule='one_failed',
    dag=dag,
)

# Conexión a Kafka
conexion_kafka_task = KubernetesPodOperator(
    namespace='default',
    image="usuario_docker/conexion-kafka:latest",
    cmds=["python", "conexion_kafka.py"],
    name="conexion_kafka_task",
    task_id="conexion_kafka_task",
    get_logs=True,
    dag=dag,
)

email_kafka_task = EmailOperator(
    task_id='enviar_email_falla_kafka',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Conexión a Kafka',
    html_content='Ha ocurrido una falla en el DAG de Conexión a Kafka.',
    trigger_rule='one_failed',
    dag=dag,
)

# Kubernetes Tasks
kubernetes_task = KubernetesPodOperator(
    namespace='default',
    image="usuario_docker/kubernetes-tasks:latest",
    cmds=["python", "kubernetes_tasks.py"],
    name="kubernetes_task",
    task_id="kubernetes_task",
    get_logs=True,
    dag=dag,
)

email_kubernetes_task = EmailOperator(
    task_id='enviar_email_falla_kubernetes',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Kubernetes',
    html_content='Ha ocurrido una falla en el DAG de Kubernetes.',
    trigger_rule='one_failed',
    dag=dag,
)

# Conexión a Spark
conexion_spark_task = KubernetesPodOperator(
    namespace='default',
    image="usuario_docker/conexion-spark:latest",
    cmds=["python", "conexion_spark.py"],
    name="conexion_spark_task",
    task_id="conexion_spark_task",
    get_logs=True,
    dag=dag,
)

email_spark_task = EmailOperator(
    task_id='enviar_email_falla_spark',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Conexión a Spark',
    html_content='Ha ocurrido una falla en el DAG de Conexión a Spark.',
    trigger_rule='one_failed',
    dag=dag,
)

# Transformación de Datos
transformar_datos_task = KubernetesPodOperator(
    namespace='default',
    image="usuario_docker/transformacion-datos:latest",
    cmds=["python", "transformacion_datos.py"],
    name="transformar_datos_task",
    task_id="transformar_datos_task",
    get_logs=True,
    dag=dag,
)

email_transformacion_task = EmailOperator(
    task_id='enviar_email_falla_transformacion',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Transformación de Datos',
    html_content='Ha ocurrido una falla en el DAG de Transformación de Datos.',
    trigger_rule='one_failed',
    dag=dag,
)

# Carga de Datos
cargar_datos_task = KubernetesPodOperator(
    namespace='default',
    image="usuario_docker/carga-datos:latest",
    cmds=["python", "cargar_datos.py"],
    name="cargar_datos_task",
    task_id="cargar_datos_task",
    get_logs=True,
    dag=dag,
)

email_carga_task = EmailOperator(
    task_id='enviar_email_falla_carga',
    to='correo_aviso_fallas@example.com',
    subject='Falla en el DAG de Carga de Datos',
    html_content='Ha ocurrido una falla en el DAG de Carga de Datos.',
    trigger_rule='one_failed',
    dag=dag,
)

# Definición del flujo de tareas
inicializar_task >> email_inicializacion_task >> conexion_kafka_task >> email_kafka_task
conexion_kafka_task >> kubernetes_task >> email_kubernetes_task
kubernetes_task >> conexion_spark_task >> email_spark_task
conexion_spark_task >> transformar_datos_task >> email_transformacion_task
transformar_datos_task >> cargar_datos_task >> email_carga_task


# Explicación del Flujo de Trabajo
# Inicialización:

# Se ejecuta la tarea de inicialización.
# Si falla, se envía un correo electrónico.
# Conexión a Kafka:

# Espera a que la inicialización termine.
# Establece la conexión a Kafka.
# Si falla, se envía un correo electrónico.
# Kubernetes Tasks:

# Espera a que la conexión a Kafka termine.
# Ejecuta las tareas relacionadas con Kubernetes.
# Si falla, se envía un correo electrónico.
# Conexión a Spark:

# Espera a que las tareas de Kubernetes terminen.
# Establece la conexión a Spark.
# Si falla, se envía un correo electrónico.
# Transformación de Datos:

# Espera a que la conexión a Spark termine.
# Transforma los datos.
# Si falla, se envía un correo electrónico.
# Carga de Datos:

# Espera a que la transformación de datos termine.
# Carga los datos en el destino final.
# Si falla, se envía un correo electrónico.


# Notas Adicionales
# Dependencias: Cada tarea está encadenada a la anterior, asegurando que se ejecuten en el orden correcto.
# Notificaciones por Correo: Cada tarea tiene una notificación por correo electrónico configurada 
# para enviarse en caso de falla.
# Modularidad: Aunque todas las tareas están en un solo archivo, cada sección del DAG es modular
#  y puede ser fácilmente ajustada o extendida.