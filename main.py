import time

from datetime import datetime, timedelta, date, timezone
from influxdb_client_3 import Point, InfluxDBError, InfluxDB3ClientQueryError
import logging
from pymodbus.exceptions import ModbusIOException
from typing import Union
from zoneinfo import ZoneInfo

from config import get_config
from influxdb import InfluxDBHandler
from sun2000 import Sun2000, Sun2000NotConnectedError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LOCAL_TZ = ZoneInfo("Europe/Bucharest")
UTC = timezone.utc
ROLLOUT_HOUR_LOCAL = 0
ROLLOUT_MINUTE_LOCAL = 1
ROLLOUT_FORCE = False


def get_last_rollup_time_utc(handler:InfluxDBHandler, rollup_type:str) -> Union[datetime, None]:
    sql = f"""
    SELECT MAX(rollup_{rollup_type}) AS last_rollup
    FROM {handler.config.influxdb_dbname_rollup_state}
    """
    try:
        table = handler.client.query(sql)
        row = table.to_pylist()[0]
        return datetime.fromisoformat(row['last_rollup'])  # UTC datetime or None
    except (InfluxDBError, InfluxDB3ClientQueryError) as e:
        if f"table 'public.iox.{handler.config.influxdb_dbname_rollup_state}' not found" in e.message or \
            f'No field named rollup_{rollup_type}' in e.message:
            logger.warning(f"Either rollup state table or rollup field not found: {handler.config.influxdb_dbname_rollup_state}. Assuming first rollup.")
            return None
        raise

def last_rollup_utc_to_local(last_rollup_utc:Union[datetime, None]) -> date:
    if last_rollup_utc:
        last_rollup_local = last_rollup_utc.astimezone(LOCAL_TZ).date()
    else:
        # first run → roll up from first data day
        last_rollup_local = datetime.now(LOCAL_TZ).date().replace(year=2025, month=12, day=20)
    return last_rollup_local

def get_days_to_rollup(last_rollup_day_local:date, latest_complete_day:date) -> list[date]:
    days_to_rollup = []
    d = last_rollup_day_local + timedelta(days=1)
    while d <= latest_complete_day:
        days_to_rollup.append(d)
        d += timedelta(days=1)
    return days_to_rollup

def get_latest_complete_day():
    now_local = datetime.now(LOCAL_TZ)
    latest_complete_day = now_local.date() - timedelta(days=1)
    return latest_complete_day

def write_rollup_state(influxdb_handler:InfluxDBHandler, day_local:date, rollup_type:str) -> None:
    t_local = datetime.combine(day_local, datetime.min.time(), LOCAL_TZ)
    t_utc = t_local.astimezone(UTC)

    logger.info(f"Writing rollup {rollup_type} state for day {day_local}")
    influxdb_handler.client.write({
        "measurement": f'{influxdb_handler.config.influxdb_dbname_rollup_state}',
        "time": t_utc.isoformat(),
        "fields": {
            f'rollup_{rollup_type}': t_local.isoformat()
        }
    })


def rollup_already_done(influxdb_handler:InfluxDBHandler, day_local:date, rollup_type:str) -> bool:
    t_local = datetime.combine(day_local, datetime.min.time(), LOCAL_TZ)
    query = f"""
    SELECT rollup_{rollup_type} AS last_rollup
    FROM {influxdb_handler.config.influxdb_dbname_rollup_state}
    WHERE rollup_{rollup_type} = TIMESTAMP '{t_local.isoformat()}'
    """
    try:
        table = influxdb_handler.client.query(query)
        if table.num_rows == 0:
            return False

        row = table.to_pylist()[0]
        last_rollup_time:Union[datetime, None] = datetime.fromisoformat(row['last_rollup'])
        if last_rollup_time is None:
            return False

        last_rollup_day_local = last_rollup_time.astimezone(LOCAL_TZ).date()
        return last_rollup_day_local == day_local
    except (InfluxDBError, InfluxDB3ClientQueryError) as e:
        if f"table 'public.iox.{influxdb_handler.config.influxdb_dbname_rollup_state}' not found" in e.message or \
            f'No field named rollup_{rollup_type}' in e.message:
            logger.warning(f"Either rollup state table or rollup field not found: {influxdb_handler.config.influxdb_dbname_rollup_state}. Assuming first rollup.")
            return False
    return True


def daily_rollup_energy_breakdown(influxdb_handler: InfluxDBHandler, rollout_day:date) -> bool:
    rollout_filter_start_utc = datetime.combine(rollout_day, datetime.min.time(), tzinfo=LOCAL_TZ).astimezone(UTC)
    rollout_filter_end_utc = rollout_filter_start_utc + timedelta(days=1)
    logger.debug(f"Rollout day: {rollout_day}")
    logger.debug(f"Rollout start utc: {rollout_filter_start_utc}")
    logger.debug(f"Rollout end utc: {rollout_filter_end_utc}")

    was_rollup_done = rollup_already_done(influxdb_handler=influxdb_handler, day_local=rollout_day, rollup_type="energy_breakdown")
    if was_rollup_done:
        logger.info(f"Energy breakdown daily rollup already done for day {rollout_day} — skipping")
        return True
    logger.info(f'Starting energy breakdown daily rollup for day {rollout_day}')
    query = f"""
    SELECT
      MAX(accumulated_energy_yield) - MIN(accumulated_energy_yield) AS pv_energy,
      MAX(meter_reverse_active_power) - MIN(meter_reverse_active_power) AS house_from_grid,
      MAX(meter_positive_active_electricity) - MIN(meter_positive_active_electricity) AS feed_in
    FROM sun2000_monitoring
    WHERE time >= TIMESTAMP '{rollout_filter_start_utc.isoformat()}'
      AND time <  TIMESTAMP '{rollout_filter_end_utc.isoformat()}'
    """
    table = influxdb_handler.client.query(query)
    if table.num_rows == 0:
        logger.error('No data for rollup day — rollup aborted')
        return False

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

    influxdb_handler.client.write(Point(influxdb_handler.config.influxdb_dbname_daily)
                                  .tag('rollup', 'energy_breakdown')
                                  .field("pv_energy", float(pv_energy))
                                  .field("house_from_grid", float(house_from_grid))
                                  .field("feed_in", float(feed_in))
                                  .field("house_from_pv", float(house_from_pv))
                                  .time(rollout_filter_end_utc.isoformat())
                                  )
    logger.info("Daily energy breakdown rollup written successfully:")
    logger.info(f"Date: {rollout_filter_end_utc.date()}")
    logger.info(f"PV energy: {pv_energy:.2f} kWh")
    logger.info(f"House from grid: {house_from_grid:.2f} kWh")
    logger.info(f"Feed-in: {feed_in:.2f} kWh")
    logger.info(f"House from PV: {house_from_pv:.2f} kWh")
    write_rollup_state(influxdb_handler=influxdb_handler, day_local=rollout_day, rollup_type="energy_breakdown")
    return True

def daily_rollup_battery(influxdb_handler: InfluxDBHandler, rollout_day:date) -> bool:
    rollout_filter_start_utc = datetime.combine(rollout_day, datetime.min.time(), tzinfo=LOCAL_TZ).astimezone(UTC)
    rollout_filter_end_utc = rollout_filter_start_utc + timedelta(days=1)
    logger.debug(f"Rollout day: {rollout_day}")
    logger.debug(f"Rollout start utc: {rollout_filter_start_utc}")
    logger.debug(f"Rollout end utc: {rollout_filter_end_utc}")

    was_rollup_done = rollup_already_done(influxdb_handler=influxdb_handler, day_local=rollout_day, rollup_type="battery")
    if was_rollup_done:
        logger.info(f"Battery daily rollup already done for day {rollout_day} — skipping")
        return True

    logger.info(f'Starting daily battery rollup for day {rollout_day}')
    query = f"""
        SELECT
          MAX(battery_total_charge) - MIN(battery_total_charge) AS battery_charge_kwh,
          MAX(battery_total_discharge) - MIN(battery_total_discharge) AS battery_discharge_kwh,
          MIN(battery_soc) AS battery_soc_min,
          MAX(battery_soc) AS battery_soc_max,
          AVG(battery_soc) AS battery_soc_avg,
          MAX(battery_unit1_battery_temperature) AS battery_temp_max
        FROM sun2000_monitoring
        WHERE time >= TIMESTAMP '{rollout_filter_start_utc.isoformat()}'
          AND time <  TIMESTAMP '{rollout_filter_end_utc.isoformat()}'
        """
    table = influxdb_handler.client.query(query)
    if table.num_rows == 0:
        logging.error('No data for rollup day — rollup aborted')
        return False

    row = {name: table.column(name)[0].as_py() for name in table.column_names}
    if row["battery_charge_kwh"] is None:
        logger.error('Cannot compute battery rollup — no battery data available')
        return False

    influxdb_handler.client.write(Point(influxdb_handler.config.influxdb_dbname_daily)
                                  .tag('rollup', 'battery')
                                  .tag('day', rollout_day.isoformat())
                                  .time(rollout_filter_end_utc.isoformat())
                                  .field('battery_charge_kwh', row['battery_charge_kwh'])
                                  .field('battery_discharge_kwh', row['battery_discharge_kwh'])
                                  .field('battery_soc_min', row['battery_soc_min'])
                                  .field('battery_soc_max', row['battery_soc_max'])
                                  .field('battery_soc_avg', row['battery_soc_avg'])
                                  .field('battery_temp_max', row['battery_temp_max'])
                                  )
    logger.info("Battery daily rollup written successfully:")
    logger.info(f"Date: {rollout_filter_end_utc.date()}")
    logger.info(f"Time: {rollout_filter_end_utc.time()}")
    logger.info(f"Battery charge: {row['battery_charge_kwh']:.2f} kWh")
    logger.info(f"Battery discharge: {row['battery_discharge_kwh']:.2f} kWh")
    logger.info(f"Battery SOC min: {row['battery_soc_min']:.2f} %")
    logger.info(f"Battery SOC max: {row['battery_soc_max']:.2f} %")
    logger.info(f"Battery SOC avg: {row['battery_soc_avg']:.2f} %")
    logger.info(f"Battery temp max: {row['battery_temp_max']:.2f} °C")
    write_rollup_state(influxdb_handler=influxdb_handler, day_local=rollout_day, rollup_type="battery")
    return True

def main():
    config = get_config()
    sun2000_client = Sun2000(config=config)
    influxdb_handler = InfluxDBHandler(config=config)
    rollup_map = {
        'energy_breakdown': daily_rollup_energy_breakdown,
        'battery': daily_rollup_battery
    }

    logger.info(f'InfluxDB ping server version: {influxdb_handler.ping()}')
    logger.info(f'Sun2000 ping server: {sun2000_client.ping()}')
    logger.info(f'Polling every {config.polling_interval_seconds} seconds')

    while True:
        try:
            points = [
                Point(influxdb_handler.config.influxdb_dbname).tag("source", register_data.source).field(attribute, register_data.value) for attribute, register_data in sun2000_client.poll_all().items()
            ]
            influxdb_handler.client.write(points, )
        except (ModbusIOException, Sun2000NotConnectedError) as e:
            logger.error(e)

        now_local = datetime.now(LOCAL_TZ)
        if (now_local.hour == ROLLOUT_HOUR_LOCAL and now_local.minute == ROLLOUT_MINUTE_LOCAL) or ROLLOUT_FORCE:
            for rollup_type, rollup_function in rollup_map.items():
                last_rollup_utc = get_last_rollup_time_utc(handler=influxdb_handler, rollup_type=rollup_type)
                last_rollup_day_local = last_rollup_utc_to_local(last_rollup_utc=last_rollup_utc)
                latest_complete_day = get_latest_complete_day()
                days_to_rollup = get_days_to_rollup(last_rollup_day_local=last_rollup_day_local, latest_complete_day=latest_complete_day)

                for rollup_day in days_to_rollup:
                    try:
                        logger.info(f'Processing rollup {rollup_type} for day: {rollup_day}')
                        rollup_function(influxdb_handler=influxdb_handler, rollout_day=rollup_day)
                    except Exception as e:
                        logger.error(f'Daily {rollup_type} rollup failed for day {rollup_day}: {e}')

        time.sleep(config.polling_interval_seconds)

if __name__ == '__main__':
    main()
