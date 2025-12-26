import time

from config import get_config
from influxdb import InfluxDBHandler
from sun2000 import Sun2000
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

while True:
    try:
        points = [
            Point(influxdb_client.config.influxdb_dbname).tag("source", register_data.source).field(attribute, register_data.value) for attribute, register_data in sun2000_client.poll_all().items()
        ]
        influxdb_client.client.write(points, )
    except ModbusIOException as e:
        logger.error(e)
    time.sleep(config.polling_interval_seconds)
