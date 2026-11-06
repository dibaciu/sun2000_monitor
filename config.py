import os
from dataclasses import dataclass, fields

@dataclass(kw_only=True)
class MonitorConfig:
    influxdb_token: str
    influxdb_host: str
    influxdb_port: int
    sun2000_inverter_host: str
    sun2000_inverter_port: int
    influxdb_dbname: str
    influxdb_dbname_daily: str
    influxdb_dbname_rollup_state: str
    polling_interval_seconds: int
    # MQTT / Home Assistant (optional: publishing is off while mqtt_host is empty)
    mqtt_host: str = ''
    mqtt_port: int = 1883
    mqtt_username: str = ''
    mqtt_password: str = ''
    mqtt_publish_interval_seconds: int = 30
    mqtt_topic_prefix: str = 'sun2000'
    mqtt_discovery_prefix: str = 'homeassistant'

OPTIONAL_FIELDS = {'mqtt_host', 'mqtt_port', 'mqtt_username', 'mqtt_password', 'mqtt_publish_interval_seconds',
                   'mqtt_topic_prefix', 'mqtt_discovery_prefix'}

def get_config():
    config = MonitorConfig(
        influxdb_token=os.environ.get('INFLUXDB_TOKEN'),
        influxdb_host=os.environ.get('INFLUXDB_HOST'),
        influxdb_port=int(os.environ.get('INFLUXDB_PORT', '8181')),
        influxdb_dbname=os.environ.get('INFLUXDB_DBNAME', 'sun2000_monitoring'),
        influxdb_dbname_daily=os.environ.get('INFLUXDB_DBNAME_DAILY', 'sun2000_monitoring_daily'),
        influxdb_dbname_rollup_state=os.environ.get('INFLUXDB_DBNAME_ROLLUP_STATE', 'sun2000_monitoring_rollup_state'),
        sun2000_inverter_host=os.environ.get('SUN2000_INVERTER_HOST'),
        sun2000_inverter_port=int(os.environ.get('SUN2000_INVERTER_PORT', '6607')),
        polling_interval_seconds=int(os.environ.get('POLLING_INTERVAL_SECONDS', '60')),
        mqtt_host=os.environ.get('MQTT_HOST', ''),
        mqtt_port=int(os.environ.get('MQTT_PORT', '1883')),
        mqtt_username=os.environ.get('MQTT_USERNAME', ''),
        mqtt_password=os.environ.get('MQTT_PASSWORD', ''),
        mqtt_publish_interval_seconds=int(os.environ.get('MQTT_PUBLISH_INTERVAL_SECONDS', '30')),
        mqtt_topic_prefix=os.environ.get('MQTT_TOPIC_PREFIX', 'sun2000'),
        mqtt_discovery_prefix=os.environ.get('MQTT_DISCOVERY_PREFIX', 'homeassistant'),
    )
    # Validate required fields
    for field in fields(config):
        if field.name not in OPTIONAL_FIELDS and getattr(config, field.name) is None:
            raise ValueError(f"Missing required configuration for {field.name}")
    return config
