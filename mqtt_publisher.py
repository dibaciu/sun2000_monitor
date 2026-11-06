"""Publish polled values to MQTT with Home Assistant MQTT discovery.

One JSON message per publish on <prefix>/state carries all values; <prefix>/status is
"online"/"offline" (the broker sets "offline" via the last will if the monitor dies).
Discovery configs are retained under <discovery_prefix>/<component>/sun2000/<key>/config.

Publishing is deliberately slower than polling (MQTT_PUBLISH_INTERVAL_SECONDS, default 30 s):
Home Assistant's recorder writes every state change to its database.
"""
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union

import paho.mqtt.client as mqtt

from config import MonitorConfig

logger = logging.getLogger(__name__)

Value = Union[str, int, float, None]

# ── status texts (same wording as the Grafana dashboard) ───────────────────────────────────

DEVICE_STATUS = {0: 'Standby: initializing', 1: 'Standby: detecting insulation resistance',
                 2: 'Standby: detecting irradiation', 3: 'Standby: grid detecting', 256: 'Starting',
                 512: 'On grid', 513: 'Grid connection: power limited', 514: 'Grid connection: self-derating',
                 515: 'Off-grid running', 40960: 'Standby: no irradiation'}
DEVICE_STATUS_RANGES = [(768, 776, 'Shutdown'), (1025, 1029, 'Grid scheduling'), (1280, 1281, 'Spot check')]
INVERTER_STATE = {0: 'Unknown', 1: 'Standby', 2: 'Grid connected', 4: 'Derating', 6: 'Fault', 8: 'Shutdown'}
BATTERY_STATUS = {0: 'Offline', 1: 'Standby', 2: 'Running', 3: 'Fault', 4: 'Sleep'}
BATTERY_MODE = {0: 'Adaptive', 1: 'Fixed charge/discharge', 2: 'Maximise self consumption',
                3: 'Time of use (LG)', 4: 'Fully fed to grid', 5: 'Time of use (LUNA2000)'}
METER_STATUS = {0: 'Offline', 1: 'Normal'}

def _text(table: Dict[int, str], ranges=()) -> Callable[[Value], Value]:
    def convert(value):
        if value is None:
            return None
        code = int(float(value))
        if code in table:
            return table[code]
        for low, high, text in ranges:
            if low <= code <= high:
                return text
        return f'Unknown ({code})'
    return convert

# ── entities ───────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Entity:
    key: str                        # key in the state JSON (= InfluxDB field name, except computed ones)
    name: str
    device: str                     # 'inverter' | 'battery' | 'meter'
    unit: Optional[str] = None
    device_class: Optional[str] = None
    state_class: Optional[str] = None
    precision: Optional[int] = None
    diagnostic: bool = False
    icon: Optional[str] = None
    convert: Optional[Callable[[Value], Value]] = None
    component: str = 'sensor'

def _power(key, name, device, **kw):  return Entity(key, name, device, 'W', 'power', 'measurement', 0, **kw)
def _energy(key, name, device, **kw): return Entity(key, name, device, 'kWh', 'energy', 'total_increasing', 2, **kw)
def _volt(key, name, device, **kw):   return Entity(key, name, device, 'V', 'voltage', 'measurement', 1, **kw)
def _amp(key, name, device, **kw):    return Entity(key, name, device, 'A', 'current', 'measurement', 2, **kw)
def _temp(key, name, device, **kw):   return Entity(key, name, device, '°C', 'temperature', 'measurement', 1, **kw)

ENTITIES: List[Entity] = [
    # inverter
    _power('active_power', 'Active power', 'inverter'),
    _power('input_power', 'DC input power', 'inverter', icon='mdi:solar-power'),
    _power('house_consumption_power', 'House consumption', 'inverter', icon='mdi:home-lightning-bolt'),
    _power('peak_active_power_of_current_day', 'Peak power today', 'inverter'),
    _energy('daily_energy_yield', 'Yield today', 'inverter', icon='mdi:solar-power'),
    _energy('accumulated_energy_yield', 'Total yield', 'inverter', icon='mdi:solar-power'),
    Entity('efficiency', 'Efficiency', 'inverter', '%', None, 'measurement', 1, icon='mdi:percent'),
    _temp('internal_temperature', 'Internal temperature', 'inverter'),
    Entity('insulation_resistance', 'Insulation resistance', 'inverter', 'MΩ', None, 'measurement', 1, diagnostic=True, icon='mdi:omega'),
    Entity('device_status', 'Status', 'inverter', icon='mdi:information-outline', convert=_text(DEVICE_STATUS, DEVICE_STATUS_RANGES)),
    Entity('state1', 'State', 'inverter', icon='mdi:state-machine', convert=_text(INVERTER_STATE)),
    Entity('problem', 'Problem', 'inverter', device_class='problem', component='binary_sensor'),
    Entity('fault_code', 'Fault code', 'inverter', diagnostic=True, icon='mdi:alert-circle-outline'),
    Entity('alarm1', 'Alarm 1', 'inverter', diagnostic=True, icon='mdi:alert-outline'),
    Entity('alarm2', 'Alarm 2', 'inverter', diagnostic=True, icon='mdi:alert-outline'),
    Entity('alarm3', 'Alarm 3', 'inverter', diagnostic=True, icon='mdi:alert-outline'),
    Entity('grid_frequency', 'Grid frequency', 'inverter', 'Hz', 'frequency', 'measurement', 2, diagnostic=True),
    Entity('power_factor', 'Power factor', 'inverter', None, 'power_factor', 'measurement', 3, diagnostic=True),
    # the inverter reports kvar; 'var' is the unit every HA version accepts for reactive_power
    Entity('reactive_power', 'Reactive power', 'inverter', 'var', 'reactive_power', 'measurement', 0, diagnostic=True,
           convert=lambda v: None if v is None else round(v * 1000)),
    *[_volt(f'phase_{p}_voltage', f'Phase {p.upper()} voltage', 'inverter', diagnostic=True) for p in 'abc'],
    *[_amp(f'phase_{p}_current', f'Phase {p.upper()} current', 'inverter', diagnostic=True) for p in 'abc'],
    # battery
    Entity('battery_soc', 'State of charge', 'battery', '%', 'battery', 'measurement', 1),
    _power('battery_charge_discharge_power', 'Charge/discharge power', 'battery', icon='mdi:battery-charging'),
    _energy('battery_total_charge', 'Total charge', 'battery', icon='mdi:battery-plus'),
    _energy('battery_total_discharge', 'Total discharge', 'battery', icon='mdi:battery-minus'),
    _energy('battery_current_day_charge_capacity', 'Charged today', 'battery', icon='mdi:battery-plus'),
    _energy('battery_current_day_discharge_capacity', 'Discharged today', 'battery', icon='mdi:battery-minus'),
    _temp('battery_unit1_battery_temperature', 'Temperature', 'battery'),
    Entity('battery_running_status', 'Status', 'battery', icon='mdi:battery-heart-variant', convert=_text(BATTERY_STATUS)),
    Entity('battery_working_mode_settings', 'Working mode', 'battery', icon='mdi:cog-outline', convert=_text(BATTERY_MODE)),
    Entity('battery_backup_power_soc', 'Backup power SOC', 'battery', '%', None, 'measurement', 1, diagnostic=True, icon='mdi:battery-lock'),
    Entity('battery_unit1_remaining_charge_discharge_time', 'Remaining charge/discharge time', 'battery', 'min', 'duration', 'measurement', 0, diagnostic=True),
    Entity('battery_unit1_fault_id', 'Fault ID', 'battery', diagnostic=True, icon='mdi:alert-circle-outline'),
    _volt('battery_bus_voltage', 'Bus voltage', 'battery', diagnostic=True),
    Entity('battery_bus_current', 'Bus current', 'battery', 'A', 'current', 'measurement', 1, diagnostic=True),
    Entity('battery_rated_capacity', 'Rated capacity', 'battery', 'Wh', 'energy_storage', None, 0, diagnostic=True),
    _power('battery_maximum_charge_power', 'Maximum charge power', 'battery', diagnostic=True),
    _power('battery_maximum_discharge_power', 'Maximum discharge power', 'battery', diagnostic=True),
    # power meter
    _power('meter_active_power', 'Grid power (+export / −import)', 'meter', icon='mdi:transmission-tower'),
    _energy('meter_reverse_active_power', 'Grid import total', 'meter', icon='mdi:transmission-tower-import'),
    _energy('meter_positive_active_electricity', 'Grid export total', 'meter', icon='mdi:transmission-tower-export'),
    Entity('meter_status', 'Status', 'meter', diagnostic=True, convert=_text(METER_STATUS)),
    Entity('meter_grid_frequency', 'Grid frequency', 'meter', 'Hz', 'frequency', 'measurement', 2, diagnostic=True),
    Entity('meter_power_factor', 'Power factor', 'meter', None, 'power_factor', 'measurement', 3, diagnostic=True),
    Entity('meter_reactive_power', 'Reactive power', 'meter', 'var', 'reactive_power', 'measurement', 0, diagnostic=True),
    *[_volt(f'meter_{p}_phase_voltage', f'Phase {p.upper()} voltage', 'meter', diagnostic=True) for p in 'abc'],
    *[_amp(f'meter_{p}_phase_current', f'Phase {p.upper()} current', 'meter', diagnostic=True) for p in 'abc'],
    *[_power(f'meter_{p}_phase_active_power', f'Phase {p.upper()} power', 'meter', diagnostic=True) for p in 'abc'],
]

def _pv_string_entities(values: Dict[str, Value]) -> List[Entity]:
    """pv<n>_power / _voltage / _current for every string present in the polled values."""
    strings = sorted({int(m.group(1)) for k in values if (m := re.fullmatch(r'pv(\d+)_(voltage|current|power)', k))})
    out = []
    for n in strings:
        out += [_power(f'pv{n}_power', f'PV string {n} power', 'inverter', icon='mdi:solar-panel'),
                _volt(f'pv{n}_voltage', f'PV string {n} voltage', 'inverter'),
                _amp(f'pv{n}_current', f'PV string {n} current', 'inverter')]
    return out

# ── publisher ──────────────────────────────────────────────────────────────────────────────

class MqttPublisher:
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.prefix = config.mqtt_topic_prefix
        self.state_topic = f'{self.prefix}/state'
        self.status_topic = f'{self.prefix}/status'
        self.interval = config.mqtt_publish_interval_seconds
        self._last_publish = 0.0
        self._discovery_needed = True
        self._entities: List[Entity] = []
        self._connected = False
        self._warned_unreachable = False

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'{self.prefix}-monitor')
        if config.mqtt_username:
            self.client.username_pw_set(config.mqtt_username, config.mqtt_password)
        self.client.will_set(self.status_topic, 'offline', qos=1, retain=True)
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def start(self) -> None:
        logger.info(f'MQTT: connecting to {self.config.mqtt_host}:{self.config.mqtt_port}, '
                    f'publishing every {self.interval}s to {self.state_topic}')
        self.client.connect_async(self.config.mqtt_host, self.config.mqtt_port, keepalive=60)
        self.client.loop_start()  # background thread, reconnects by itself

    def stop(self) -> None:
        try:
            self.client.publish(self.status_topic, 'offline', qos=1, retain=True).wait_for_publish(2)
        finally:
            self.client.loop_stop()
            self.client.disconnect()

    # callbacks run in paho's network thread: only set flags / publish
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error(f'MQTT: connection refused by broker: {reason_code} (check MQTT_USERNAME / MQTT_PASSWORD)')
            return
        logger.info('MQTT: connected')
        self._connected = True
        self._warned_unreachable = False
        client.publish(self.status_topic, 'online', qos=1, retain=True)
        client.subscribe(f'{self.config.mqtt_discovery_prefix}/status')  # HA announces restarts here
        self._discovery_needed = True

    def _on_connect_fail(self, client, userdata):
        if not self._warned_unreachable:  # paho keeps retrying; say it once until it works
            logger.warning(f'MQTT: broker {self.config.mqtt_host}:{self.config.mqtt_port} not reachable, retrying in the background')
            self._warned_unreachable = True

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if self._connected:
            logger.warning(f'MQTT: disconnected ({reason_code}), reconnecting in the background')
        self._connected = False

    def _on_message(self, client, userdata, message):
        if message.topic.endswith('/status') and message.payload == b'online':
            logger.info('MQTT: Home Assistant restarted, re-sending discovery')
            self._discovery_needed = True

    # called from the polling loop every cycle
    def publish(self, values: Dict[str, Value]) -> None:
        now = time.monotonic()
        if not self._connected or now - self._last_publish < self.interval:
            return
        if not values.get('sn'):
            return  # device info comes with the first full poll
        entities = ENTITIES + _pv_string_entities(values)
        if self._discovery_needed or entities != self._entities:
            self._publish_discovery(entities, values)
        state = {}
        for e in entities:
            v = self._computed(e.key, values) if e.key == 'problem' else values.get(e.key)
            state[e.key] = e.convert(v) if e.convert else v
        self.client.publish(self.state_topic, json.dumps(state, separators=(',', ':')), qos=0, retain=False)
        self._last_publish = now

    @staticmethod
    def _computed(key: str, values: Dict[str, Value]) -> Value:
        parts = [values.get(k) for k in ('alarm1', 'alarm2', 'alarm3', 'fault_code')]
        if all(p is None for p in parts):
            return None
        return 'ON' if any(p not in (None, 0, 0.0) for p in parts) else 'OFF'

    def _devices(self, values: Dict[str, Value]) -> Dict[str, dict]:
        sn = str(values['sn'])
        inverter_id = f'sun2000_{sn}'
        return {
            'inverter': {'identifiers': [inverter_id], 'name': 'SUN2000 Inverter', 'manufacturer': 'Huawei',
                         'model': values.get('model') or 'SUN2000', 'serial_number': sn,
                         'sw_version': values.get('software_version') or None},
            'battery': {'identifiers': [f'{inverter_id}_battery'], 'name': 'SUN2000 Battery', 'manufacturer': 'Huawei',
                        'model': 'LUNA2000', 'via_device': inverter_id},
            'meter': {'identifiers': [f'{inverter_id}_meter'], 'name': 'SUN2000 Power meter', 'manufacturer': 'Huawei',
                      'model': 'Power meter', 'via_device': inverter_id},
        }

    def _publish_discovery(self, entities: List[Entity], values: Dict[str, Value]) -> None:
        devices = self._devices(values)
        sn = str(values['sn'])
        expire = max(3 * self.interval, 120)
        dp = self.config.mqtt_discovery_prefix
        for e in entities:
            payload = {
                'name': e.name,
                'unique_id': f'sun2000_{sn}_{e.key}',
                'state_topic': self.state_topic,
                'value_template': f'{{{{ value_json.{e.key} }}}}',
                'availability_topic': self.status_topic,
                'device': {k: v for k, v in devices[e.device].items() if v is not None},
                'expire_after': expire,
            }
            if e.component == 'binary_sensor':
                payload.update(payload_on='ON', payload_off='OFF')
            for k, v in (('unit_of_measurement', e.unit), ('device_class', e.device_class), ('state_class', e.state_class),
                         ('suggested_display_precision', e.precision), ('icon', e.icon)):
                if v is not None:
                    payload[k] = v
            if e.diagnostic:
                payload['entity_category'] = 'diagnostic'
            self.client.publish(f'{dp}/{e.component}/sun2000/{e.key}/config', json.dumps(payload), qos=1, retain=True)
        # entities that existed before but no longer (e.g. fewer PV strings): remove them from HA
        for old in {e.key: e for e in self._entities}.keys() - {e.key for e in entities}:
            comp = next(e.component for e in self._entities if e.key == old)
            self.client.publish(f'{dp}/{comp}/sun2000/{old}/config', '', qos=1, retain=True)
        self._entities = entities
        self._discovery_needed = False
        logger.info(f'MQTT: sent Home Assistant discovery for {len(entities)} entities')
