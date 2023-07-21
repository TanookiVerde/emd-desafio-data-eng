import pytz
from datetime import timedelta, datetime

from prefect.schedules import IntervalSchedule


brt_gps_extraction_schedule = IntervalSchedule(
    start_date  = datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(seconds=1),
    interval    = timedelta(minutes=1),
    end_date    = datetime.now(pytz.timezone('America/Sao_Paulo')) + timedelta(minutes=10)
)