import os
from dataclasses import dataclass

@dataclass(kw_only=True)
class MonitorConfig:
    influxdb_token: str
    influxdb_host: str
    influxdb_port: int
    sun2000_inverter_host: str
    sun2000_inverter_port: int
    influxdb_dbname: str
    polling_interval_seconds: int

def get_config():
    config = MonitorConfig(
        influxdb_token=os.environ.get('INFLUXDB_TOKEN'),
        influxdb_host=os.environ.get('INFLUXDB_HOST'),
        influxdb_port=int(os.environ.get('INFLUXDB_PORT', '8181')),
        influxdb_dbname=os.environ.get('INFLUXDB_DBNAME', 'sun2000_data'),
        sun2000_inverter_host=os.environ.get('SUN2000_INVERTER_HOST'),
        sun2000_inverter_port=int(os.environ.get('SUN2000_INVERTER_PORT', '6607')),
        polling_interval_seconds=int(os.environ.get('POLLING_INTERVAL_SECONDS', '60'))
    )
    # Validate required fields
    for field in config.__dataclass_fields__.values():
        if getattr(config, field.name) is None:
            raise ValueError(f"Missing required configuration for {field.name}")
    return config
