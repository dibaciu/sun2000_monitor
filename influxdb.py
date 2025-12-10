from config import MonitorConfig
from influxdb_client_3 import InfluxDBClient3, Point

class InfluxDBHandler:
    def __init__(self, config:MonitorConfig):
        self.config = config
        self.client = InfluxDBClient3(
            host=f'http://{config.influxdb_host}:{config.influxdb_port}',
            token=config.influxdb_token,
            database=config.influxdb_dbname
        )

    def ping(self):
        try:
            version = self.client.get_server_version()
            return version
        except Exception as e:
            raise ConnectionError(f"Failed to connect to InfluxDB: {e}")

    def get_databases(self):
        query = 'select database_name from system.databases'
        try:
            databases = self.client.query(query=query, database='_internal')
            return databases
        except Exception as e:
            raise ConnectionError(f"Failed to retrieve databases: {e}")
