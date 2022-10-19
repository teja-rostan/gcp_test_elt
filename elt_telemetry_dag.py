import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils.task_group import TaskGroup

# from airflow.providers.google.cloud.operators.dataform import (
#     DataformCreateCompilationResultOperator,
#     DataformCreateWorkflowInvocationOperator,
# )

# GCS configs
BUCKET_NAME = "{{var.value.gcs_bucket}}"

# Dataform configs
PROJECT_ID = ""
REPOSITORY_ID = "telemetry_US"
REGION = "US"

# Dag configs
DAG_NAME = "elt_telemetry"


# time variables
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    "start_date": yesterday,
    # When depends_on_past is set to True, keeps a task from getting 
    # triggered if the previous schedule for the task hasn’t succeeded.
    "depends_on_past": True,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
    # If task fails, retires again (1 time) with the delay (30 min).
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=30),
}


with models.DAG(
    dag_id=DAG_NAME,
    # When start_date is before current date, the job runs immediatelly 
    # after it is uploaded.
    start_date=yesterday, 
    schedule_interval=datetime.timedelta(days=1), # Runs daily, after midnight.
    default_args=default_dag_args,
) as dag:


    with TaskGroup(group_id='Extract') as extract:
        get_wialon_csv = GCSToBigQueryOperator(
            task_id='Get_Wialon',
            bucket=BUCKET_NAME,
            source_objects=["wialon_dump.csv"],
            destination_project_dataset_table="datalake.wialon_dump",
            source_format="CSV",
            schema_fields=[
                {"name": "unit_id", "type": "STRING"},
                {"name": "dateTime", "type": "STRING"},
                {"name": "driverId", "type": "STRING"},
                {"name": "gpsLongitude", "type": "STRING"},
                {"name": "gpsLatitude", "type": "STRING"},
                {"name": "speed", "type": "STRING"},
                {"name": "altitude", "type": "STRING"},
                {"name": "course", "type": "STRING"},
            ],
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_telematics_csv = GCSToBigQueryOperator(
            task_id='Get_Telematics',
            bucket=BUCKET_NAME,
            source_objects=["telematics_dump.csv"],
            destination_project_dataset_table="datalake.telematics_dump",
            source_format="CSV",
            schema_fields=[
                {"name": "DateTime", "type": "STRING"},
                {"name": "SerialNumber", "type": "STRING"},
                {"name": "GpsLongitude", "type": "STRING"},
                {"name": "GpsLatitude", "type": "STRING"},
                {"name": "TotalWorkingHours", "type": "STRING"},
                {"name": "Engine_rpm", "type": "STRING"},
                {"name": "EngineLoad", "type": "STRING"},
                {"name": "FuelConsumption_l_h", "type": "STRING"},
                {"name": "SpeedGearbox_km_h", "type": "STRING"},
                {"name": "SpeedRadar_km_h", "type": "STRING"},
                {"name": "TempCoolant_C", "type": "STRING"},
                {"name": "PtoFront_rpm", "type": "STRING"},
                {"name": "PtoRear_rpm", "type": "STRING"},
                {"name": "GearShift", "type": "STRING"},
                {"name": "TempAmbient_C", "type": "STRING"},
                {"name": "ParkingBreakStatus", "type": "STRING"},
                {"name": "DifferentialLockStatus", "type": "STRING"},
                {"name": "AllWheelDriveStatus", "type": "STRING"},
                {"name": "CreeperStatus", "type": "STRING"},
            ],
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_fendt_gps_json = GCSToBigQueryOperator(
            task_id='Get_Fendt_GPS',
            bucket=BUCKET_NAME,
            source_objects=["fendt_gps.json"],
            destination_project_dataset_table="datalake.fendt_gps",
            source_format="NEWLINE_DELIMITED_JSON",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_fendt_partitioned_json = GCSToBigQueryOperator(
            task_id='Get_Fendt_Partitioned',
            bucket=BUCKET_NAME,
            source_objects=["fendt_data_partitioned.json"],
            destination_project_dataset_table="datalake.fendt_data_partitioned",
            source_format="NEWLINE_DELIMITED_JSON",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

    # create_compilation_result = DataformCreateCompilationResultOperator(
    #     task_id="create_compilation_result",
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     repository_id=REPOSITORY_ID,
    #     compilation_result={
    # #        "git_commitish": GIT_COMMITISH,
    #     },
    # )

    # create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
    #     task_id='create_workflow_invocation',
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     repository_id=REPOSITORY_ID,
    #      workflow_invocation={
    #         "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
    #     },
    # )

extract #>> create_compilation_result >> create_workflow_invocation
