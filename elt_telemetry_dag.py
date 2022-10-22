import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator


def _merge_sql(source, target, columns):
    return f"""
        MERGE `{target}` T
        USING `{source}` S
        ON T.cdc = TO_BASE64(MD5(TO_JSON_STRING(S)))
        WHEN NOT MATCHED THEN
          INSERT ({columns}, inserted_at, cdc)
          VALUES ({columns}, CURRENT_DATETIME(), TO_BASE64(MD5(TO_JSON_STRING(S))))
    """



# GCS configs
BUCKET_NAME = "{{var.value.gcs_bucket}}"

# Dataform configs
PROJECT_ID = "{{var.value.gcp_project}}"
REPOSITORY_ID = "telemetry_US"
REGION = "US"

# Dag configs
DAG_NAME = "elt_telemetry"

# Schemas
WIALON_SCHEMA = [{"name": "unit_id", "type": "STRING"},
                {"name": "dateTime", "type": "STRING"},
                {"name": "driverId", "type": "STRING"},
                {"name": "gpsLongitude", "type": "STRING"},
                {"name": "gpsLatitude", "type": "STRING"},
                {"name": "speed", "type": "STRING"},
                {"name": "altitude", "type": "STRING"},
                {"name": "course", "type": "STRING"},]   
TELEMATICS_SCHEMA = [{"name": "DateTime", "type": "STRING"},
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
                {"name": "CreeperStatus", "type": "STRING"},]
FENDT_GPS_COLUMNS = "machineId, route"
FENDT_PARTITIONED_COLUMNS = "machineId, count, datas"


# time variables
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1), datetime.datetime.min.time()
)

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    "start_date": yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    "email_on_failure": False,
    "email_on_retry": False,
}

with models.DAG(
    dag_id=DAG_NAME,
    start_date=yesterday,
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:

    with TaskGroup(group_id='Extract') as extract:
        get_wialon_csv = GCSToBigQueryOperator(
            task_id='Get_Wialon',
            bucket=BUCKET_NAME,
            source_objects=["wialon_dump.csv"],
            destination_project_dataset_table="stage.wialon_dump",
            source_format="CSV",
            schema_fields=WIALON_SCHEMA,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_telematics_csv = GCSToBigQueryOperator(
            task_id='Get_Telematics',
            bucket=BUCKET_NAME,
            source_objects=["telematics_dump.csv"],
            destination_project_dataset_table="stage.telematics_dump",
            source_format="CSV",
            schema_fields=TELEMATICS_SCHEMA,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_fendt_gps_json = GCSToBigQueryOperator(
            task_id='Get_Fendt_GPS',
            bucket=BUCKET_NAME,
            source_objects=["fendt_gps.json"],
            destination_project_dataset_table="stage.fendt_gps",
            source_format="NEWLINE_DELIMITED_JSON",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

        get_fendt_partitioned_json = GCSToBigQueryOperator(
            task_id='Get_Fendt_Partitioned',
            bucket=BUCKET_NAME,
            source_objects=["fendt_data_partitioned.json"],
            destination_project_dataset_table="stage.fendt_data_partitioned", 
            source_format="NEWLINE_DELIMITED_JSON",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        )

    with TaskGroup(group_id='Load') as load:
        upsert_wialon = BigQueryInsertJobOperator(
            task_id="Upsert_Wialon",
            configuration={
                "query": {
                    "query": _merge_sql("stage.telematics_dump", "datalake.telematics_dump", ", ".join([item["name"] for item in TELEMATICS_SCHEMA])),
                "useLegacySql": False,
                }
             },
        )

        upsert_telematics = BigQueryInsertJobOperator(
            task_id="Upsert_Telematics",
            configuration={
                "query": {
                    "query": _merge_sql("stage.wialon_dump", "datalake.wialon_dump", ", ".join([item["name"] for item in WIALON_SCHEMA])),
                "useLegacySql": False,
                }
             },
        )
    
        upsert_fendt_gps = BigQueryInsertJobOperator(
            task_id="Upsert_Fendt_GPS",
            configuration={
                "query": {
                    "query": _merge_sql("stage.fendt_gps", "datalake.fendt_gps", FENDT_GPS_COLUMNS),
                "useLegacySql": False,
                }
             },
        )

        upsert_fendt_partitioned = BigQueryInsertJobOperator(
            task_id="Upsert_Fendt_Partitioned",
            configuration={
                "query": {
                    "query": _merge_sql("stage.fendt_data_partitioned", "datalake.fendt_data_partitioned", FENDT_PARTITIONED_COLUMNS),
                "useLegacySql": False,
                }
             },
        )

    transform = DataformCreateWorkflowInvocationOperator(
        task_id='Run_Dataform',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

extract >> load >> transform
