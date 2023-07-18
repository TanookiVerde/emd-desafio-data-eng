import pytz
from datetime import timedelta, datetime

from prefect import Flow
from prefect.run_configs import DockerRun
from prefect.schedules import IntervalSchedule

from pipelines.brt.tasks import \
    extract_brt_gps_data_json, \
    transform_json_data_to_df, \
    transform_identify_empty_values, \
    load_data_to_csv, \
    load_data_to_db


# FLOW - Dados GPS BRT em CSV
schedule = IntervalSchedule(
    start_date  = datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(seconds=1),
    interval    = timedelta(minutes=1),
    end_date    = datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(minutes=10)
)

run_config = DockerRun(image="test:latest")

with Flow("ETL - Dados GPS BRT", schedule=schedule, run_config=run_config) as flow:
    # NOTE: Data Extraction
    brt_gps_data_json = extract_brt_gps_data_json()

    # NOTE: Data Transformation
    brt_gps_data_df = transform_json_data_to_df(brt_gps_data_json)
    brt_gps_data_df = transform_identify_empty_values(brt_gps_data_df)

    # NOTE: Data Loading
    load_data_to_csv(brt_gps_data_df)
