import time

from config import get_config
from datetime import datetime, timedelta, timezone, date
from influxdb import InfluxDBHandler
from sun2000 import Sun2000, Sun2000NotConnectedError
from influxdb_client_3 import Point
import logging
from pymodbus.exceptions import ModbusIOException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

config = get_config()
sun2000_client = Sun2000(config=config)
influxdb_client = InfluxDBHandler(config=config)

logger.info(f'InfluxDB ping server version: {influxdb_client.ping()}')
logger.info(f'Sun2000 ping server: {sun2000_client.ping()}')
logger.info(f'Polling every {config.polling_interval_seconds} seconds')

def daily_rollup(influxdb_client: InfluxDBHandler, rollout_day:datetime=None):
    # =========================
    # TIME RANGE (previous_day)
    # =========================
    if rollout_day is None:
        rollout_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    previous_day = rollout_day - timedelta(days=1)

    # =========================
    # FlightSQL query
    # =========================
    query = f"""
    SELECT
      MAX(accumulated_energy_yield) - MIN(accumulated_energy_yield) AS pv_energy,
      MAX(meter_reverse_active_power) - MIN(meter_reverse_active_power) AS house_from_grid,
      MAX(meter_positive_active_electricity) - MIN(meter_positive_active_electricity) AS feed_in
    FROM sun2000_monitoring
    WHERE time >= TIMESTAMP '{previous_day.isoformat()}'
      AND time <  TIMESTAMP '{rollout_day.isoformat()}'
    """
    table = influxdb_client.client.query(query)
    if table.num_rows == 0:
        raise RuntimeError("No data for yesterday — rollup aborted")

    row = {name: table.column(name)[0].as_py() for name in table.column_names}

    pv_energy = row['pv_energy']
    house_from_grid = row['house_from_grid']
    feed_in = row['feed_in']
    if pv_energy is None:
        pv_energy = 0.0
    if house_from_grid is None:
        house_from_grid = 0.0
    if feed_in is None:
        feed_in = 0.0
    house_from_pv = pv_energy - feed_in

    influxdb_client.client.write(Point(influxdb_client.config.influxdb_dbname_daily)
                          .field("pv_energy", float(pv_energy))
                          .field("house_from_grid", float(house_from_grid))
                          .field("feed_in", float(feed_in))
                          .field("house_from_pv", float(house_from_pv))
                          .time(previous_day.isoformat())
                          )
    print("Daily rollup written successfully:")
    print(f"Date: {previous_day.date()}")
    print(f"PV energy: {pv_energy:.2f} kWh")
    print(f"House from grid: {house_from_grid:.2f} kWh")
    print(f"Feed-in: {feed_in:.2f} kWh")
    print(f"House from PV: {house_from_pv:.2f} kWh")


def main():
    last_rollout_day = date.today() - timedelta(days=1)
    while True:
        try:
            points = [
                Point(influxdb_client.config.influxdb_dbname).tag("source", register_data.source).field(attribute, register_data.value) for attribute, register_data in sun2000_client.poll_all().items()
            ]
            influxdb_client.client.write(points, )
        except (ModbusIOException, Sun2000NotConnectedError) as e:
            logger.error(e)
        previous_day = date.today() - timedelta(days=1)
        if last_rollout_day < previous_day:
            try:
                for day in range((previous_day - last_rollout_day).days):
                    logger.info(f"Processing rollup for day: {last_rollout_day + timedelta(days=day+2)}")
                    daily_rollup(influxdb_client=influxdb_client, rollout_day=datetime.combine(last_rollout_day + timedelta(days=day + 1), datetime.min.time(), tzinfo=timezone.utc))
                    last_rollout_day = date.today() - timedelta(days=1)
            except Exception as e:
                logger.error(f"Daily rollup failed: {e}")
        time.sleep(config.polling_interval_seconds)

if __name__ == "__main__":
    main()
