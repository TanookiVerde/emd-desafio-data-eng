import pytz
from datetime import timedelta, datetime

from prefect import Flow
from prefect.run_configs import DockerRun
from prefect.schedules import IntervalSchedule

from pipelines.brt.tasks import \
    extract_brt_gps_data_json, \
    transform_json_data_to_df, \
    load_brt_gps_data


schedule = IntervalSchedule(
    start_date  = datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(seconds=1),
    interval    = timedelta(minutes=1)
)

with Flow("ETL - Dados GPS BRT", schedule=schedule) as flow:
    brt_gps_data_json = extract_brt_gps_data_json()
    brt_gps_data_df = transform_json_data_to_df(brt_gps_data_json)
    load_brt_gps_data(brt_gps_data_df)

flow.run_config = DockerRun(
    image="test:latest"
)