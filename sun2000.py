import logging
import time
from dataclasses import dataclass
from typing import Union, Dict, List, Tuple

from pymodbus.exceptions import ConnectionException, ModbusIOException
from sun2000_modbus import datatypes
from sun2000_modbus import inverter
from sun2000_modbus import registers

from config import MonitorConfig

logger = logging.getLogger(__name__)

# Block reads: neighbouring registers are fetched with one Modbus request instead of one each.
# Huawei documents a per-request limit; 64 registers stays well inside it on the models I know of.
MAX_REGISTERS_PER_READ = 64
# Registers up to this many addresses apart are merged into one read (the gap is read and discarded).
MAX_GAP = 16
# Values that only change with a firmware update or a hardware change: read at startup and then
# every STATIC_REFRESH_SECONDS; in between the cached value is written to InfluxDB every cycle.
STATIC_ATTRIBUTES = frozenset({
    "model", "sn", "firmware_version", "software_version",
    "rated_power", "maximum_active_power", "maximum_apparent_power",
    "battery_rated_capacity", "meter_meter_type",
})
STATIC_REFRESH_SECONDS = 6 * 3600

Register = Union[registers.InverterEquipmentRegister, registers.BatteryEquipmentRegister, registers.MeterEquipmentRegister]

@dataclass()
class RegisterData:
    source: str
    value: Union[str, int, float, None]

class Sun2000NotConnectedError(Exception):
    pass

class Sun2000ReadError(Exception):
    """The inverter answered, but rejected the request (Modbus exception response)."""
    pass

class _Probe(Exception):
    """Internal: used once at startup to learn which register each property reads."""
    def __init__(self, register):
        self.register = register

# Errors that mean the link to the inverter is broken: abort the cycle and reconnect.
CONNECTION_ERRORS = (Sun2000NotConnectedError, ConnectionException, ModbusIOException)

class Sun2000:
    def __init__(self, config:MonitorConfig)->None:
        self.config = config
        self.inverter = inverter.Sun2000(host=config.sun2000_inverter_host, port=config.sun2000_inverter_port)
        self._cache: Dict[Register, Union[str, int, float]] = {}
        self._static: Dict[str, RegisterData] = {}
        self._static_read_at: Union[float, None] = None
        self._unreadable_blocks: set = set()
        self._failing: set = set()
        self._probing = False
        self.registers_to_poll = [
            "model",
            "sn",
            "firmware_version",
            "software_version",
            "rated_power",
            "maximum_active_power",
            "maximum_apparent_power",
            "state1",
            "state2",
            "state3",
            "peak_active_power_of_current_day",
            "active_power",
            "reactive_power",
            "power_factor",
            "grid_frequency",
            "efficiency",
            "internal_temperature",
            "device_status",
            "accumulated_energy_yield",
            "daily_energy_yield",
            "battery_running_status",
            "battery_working_mode_settings",
            "battery_charge_discharge_power",
            "battery_rated_capacity",
            "battery_soc",
            "battery_backup_power_soc",
            "battery_unit1_battery_temperature",
            "battery_total_charge",
            "battery_total_discharge",
            "battery_current_day_charge_capacity",
            "battery_current_day_discharge_capacity",
            "meter_status",
            "meter_a_phase_voltage",
            "meter_b_phase_voltage",
            "meter_c_phase_voltage",
            "meter_a_phase_current",
            "meter_b_phase_current",
            "meter_c_phase_current",
            "meter_active_power",
            "meter_reactive_power",
            "meter_power_factor",
            "meter_grid_frequency",
            "meter_positive_active_electricity",
            "meter_reverse_active_power",
            "meter_meter_type",
            "meter_a_phase_active_power",
            "meter_b_phase_active_power",
            "meter_c_phase_active_power",
        ]
        self._attribute_registers = self._discover_registers()
        self._dynamic_blocks = self._plan_blocks(
            [self._attribute_registers[a] for a in self.registers_to_poll if a not in STATIC_ATTRIBUTES])
        self._static_blocks = self._plan_blocks(
            [self._attribute_registers[a] for a in self.registers_to_poll if a in STATIC_ATTRIBUTES])
        logger.info(f'Polling {len(self.registers_to_poll)} values: {len(self._dynamic_blocks)} block reads per cycle, '
                    f'{len(self._static_blocks)} more every {STATIC_REFRESH_SECONDS // 3600} h for static values')

    # ── connection ──────────────────────────────────────────────────────────────────────────

    def ping(self)->bool:
        self.inverter.connect()
        return self.inverter.isConnected()

    def _ensure_connected(self) -> None:
        if not self.inverter.isConnected():
            self.inverter.connect()
            if not self.inverter.isConnected():
                raise Sun2000NotConnectedError(f'cannot connect to inverter at {self.config.sun2000_inverter_host}:{self.config.sun2000_inverter_port}')

    def reset(self) -> None:
        """Drop the TCP connection so the next cycle starts with a fresh one."""
        try:
            self.inverter.disconnect()
        except Exception:
            pass

    # ── reading ─────────────────────────────────────────────────────────────────────────────

    def _read_registers(self, address: int, count: int) -> bytes:
        """Raw bytes of `count` holding registers starting at `address`."""
        self._ensure_connected()
        response = self.inverter.inverter.read_holding_registers(address=address, count=count, slave=self.inverter.slave)
        if isinstance(response, ModbusIOException):
            raise response
        if response.isError():
            raise Sun2000ReadError(f'registers {address}..{address + count - 1}: {response}')
        return b''.join(r.to_bytes(2, byteorder='big') for r in response.registers)

    @staticmethod
    def _decode(register: Register, raw: bytes) -> Union[str, int, float]:
        # Same decoding as sun2000_modbus.Sun2000.read(), so values and types stay identical.
        value = datatypes.decode(raw, register.value.data_type)
        return value if register.value.gain is None else value / register.value.gain

    def read_data(self, register: Register, read_formatted: bool = False) -> Union[str, int, float, None]:
        if self._probing:
            raise _Probe(register)
        if not read_formatted and register in self._cache:
            return self._cache[register]
        if read_formatted:
            self._ensure_connected()
            try:
                return self.inverter.read_formatted(register=register)
            except ValueError as e:
                if 'Inverter is not connected' in str(e):
                    raise Sun2000NotConnectedError from e
                raise
        # Not prefetched (e.g. its block was rejected): read this one register on its own.
        return self._decode(register, self._read_registers(register.value.address, register.value.quantity))

    def _discover_registers(self) -> Dict[str, Register]:
        """Map each polled attribute to the register its property reads (every property calls
        read_data() first, so a probing read_data() reveals it without touching the inverter)."""
        found = {}
        self._probing = True
        try:
            for attribute in self.registers_to_poll:
                try:
                    getattr(self, attribute)
                except _Probe as probe:
                    found[attribute] = probe.register
                else:
                    raise RuntimeError(f'property {attribute} does not call read_data()')
        finally:
            self._probing = False
        return found

    @staticmethod
    def _plan_blocks(regs: List[Register]) -> List[Tuple[int, int, Tuple[Register, ...]]]:
        """Group registers into (start, count, registers) blocks for single Modbus reads."""
        blocks = []
        for reg in sorted(set(regs), key=lambda r: r.value.address):
            start, end = reg.value.address, reg.value.address + reg.value.quantity
            if blocks:
                b_start, b_count, b_regs = blocks[-1]
                b_end = b_start + b_count
                if start - b_end <= MAX_GAP and max(end, b_end) - b_start <= MAX_REGISTERS_PER_READ:
                    blocks[-1] = (b_start, max(end, b_end) - b_start, b_regs + (reg,))
                    continue
            blocks.append((start, end - start, (reg,)))
        return blocks

    def _prefetch(self, blocks) -> None:
        for start, count, regs in blocks:
            if (start, count) in self._unreadable_blocks:
                continue  # values are read one by one in read_data()
            try:
                raw = self._read_registers(start, count)
            except Sun2000ReadError as e:
                logger.warning(f'Block read rejected, reading these {len(regs)} values one by one from now on: {e}')
                self._unreadable_blocks.add((start, count))
                continue
            for reg in regs:
                offset = (reg.value.address - start) * 2
                self._cache[reg] = self._decode(reg, raw[offset:offset + reg.value.quantity * 2])

    def poll_all(self) -> Dict[str, RegisterData]:
        """Read all values. Connection problems raise (caller reconnects); a value that fails on
        its own is left out of this cycle and logged once until it works again."""
        refresh_static = self._static_read_at is None or time.monotonic() - self._static_read_at > STATIC_REFRESH_SECONDS
        if refresh_static:
            self._unreadable_blocks.clear()  # give rejected blocks another chance now and then
        self._cache = {}
        result: Dict[str, RegisterData] = {}
        try:
            self._prefetch(self._dynamic_blocks + (self._static_blocks if refresh_static else []))
            for attribute in self.registers_to_poll:
                if attribute in STATIC_ATTRIBUTES and not refresh_static:
                    if attribute in self._static:  # a static value that failed is retried at the next refresh
                        result[attribute] = self._static[attribute]
                    continue
                try:
                    result[attribute] = getattr(self, attribute)
                except CONNECTION_ERRORS:
                    raise
                except Exception as e:
                    if attribute not in self._failing:
                        logger.warning(f'Skipping {attribute}: {e!r} (logged once until it recovers)')
                        self._failing.add(attribute)
                    continue
                if attribute in self._failing:
                    logger.info(f'{attribute} readable again')
                    self._failing.discard(attribute)
                if attribute in STATIC_ATTRIBUTES:
                    self._static[attribute] = result[attribute]
        finally:
            self._cache = {}
        if refresh_static:
            self._static_read_at = time.monotonic()
        return result

    @property
    def model(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.Model))

    @property
    def sn(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.SN))

    @property
    def pn(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.PN))

    @property
    def firmware_version(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.FirmwareVersion))

    @property
    def software_version(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.SoftwareVersion))

    @property
    def protocol_version(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.ProtocolVersion))

    @property
    def model_id(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.ModelID))

    @property
    def rated_power(self, source='inverter')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.RatedPower))

    @property
    def maximum_active_power(self, source='inverter')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.MaximumActivePower))

    @property
    def maximum_apparent_power(self, source='inverter')->RegisterData:
        # kVA
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.MaximumApparentPower))

    @property
    def state1(self, source='inverter')->RegisterData:
        state1_int = int(self.read_data(registers.InverterEquipmentRegister.State1), 2)
        if state1_int & (1 << 6):
            return RegisterData(source, '6')  # Fault
        if state1_int & (1 << 8):
            return RegisterData(source, '8')  # Shutdown
        if state1_int & (1 << 3) or state1_int & (1 << 4):
            return RegisterData(source, '4')  # Derating
        if state1_int & (1 << 1):
            return RegisterData(source, '2')  # Grid connected
        if state1_int & (1 << 0):
            return RegisterData(source, '1')  # Standby
        return RegisterData(source, '0')  # Unknown

    @property
    def state2(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.State2))

    @property
    def state3(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.State3))

    @property
    def peak_active_power_of_current_day(self, source='inverter')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.PeakActivePowerOfCurrentDay))

    @property
    def active_power(self, source='inverter')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.ActivePower))

    @property
    def input_power(self, source='inverter')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.InputPower))

    @property
    def reactive_power(self, source='inverter')->RegisterData:
        # kvar
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.ReactivePower))

    @property
    def power_factor(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.PowerFactor))

    @property
    def grid_frequency(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.GridFrequency))

    @property
    def efficiency(self, source='inverter')->RegisterData:
        # %
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.Efficiency))

    @property
    def internal_temperature(self, source='inverter')->RegisterData:
        # C
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.InternalTemperature))

    @property
    def device_status(self, source='inverter')->RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.DeviceStatus))

    @property
    def accumulated_energy_yield(self, source='inverter') -> RegisterData:
        # kWh
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.AccumulatedEnergyYield))

    @property
    def daily_energy_yield(self, source='inverter') -> RegisterData:
        return RegisterData(source, self.read_data(registers.InverterEquipmentRegister.DailyEnergyYield))

    @property
    def battery_running_status(self, source='battery')->RegisterData:
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.RunningStatus))

    @property
    def battery_working_mode_settings(self, source='battery')->RegisterData:
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.WorkingModeSettings))

    @property
    def battery_charge_discharge_power(self, source='battery')->RegisterData:
        # W
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.ChargeDischargePower))

    @property
    def battery_rated_capacity(self, source='battery')->RegisterData:
        # Wh
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.RatedCapacity))

    @property
    def battery_soc(self, source='battery')->RegisterData:
        # %
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.SOC))

    @property
    def battery_backup_power_soc(self, source='battery')->RegisterData:
        # %
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.BackupPowerSOC))

    @property
    def battery_unit1_battery_temperature(self, source='battery')->RegisterData:
        # C
        data = self.read_data(registers.BatteryEquipmentRegister.Unit1BatteryTemperature)
        if int(data) > 100:
            logger.warning(f'Battery unit 1 temperature reading seems invalid: {data}')
            return RegisterData(source, None)
        return RegisterData(source, data)

    @property
    def battery_total_charge(self, source='battery')->RegisterData:
        # kWh
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.TotalCharge))

    @property
    def battery_total_discharge(self, source='battery')->RegisterData:
        # kWh
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.TotalDischarge))

    @property
    def battery_current_day_charge_capacity(self, source='battery')->RegisterData:
        # kWh
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.CurrentDayChargeCapacity))

    @property
    def battery_current_day_discharge_capacity(self, source='battery')->RegisterData:
        # kWh
        return RegisterData(source, self.read_data(registers.BatteryEquipmentRegister.CurrentDayDischargeCapacity))

    @property
    def meter_status(self, source='meter')->RegisterData:
        """
        0: offline
        1: normal
        """
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.MeterStatus))

    @property
    def meter_a_phase_voltage(self, source='meter')->RegisterData:
        # V
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.APhaseVoltage))

    @property
    def meter_b_phase_voltage(self, source='meter')->RegisterData:
        # V
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.BPhaseVoltage))

    @property
    def meter_c_phase_voltage(self, source='meter')->RegisterData:
        # V
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.CPhaseVoltage))

    @property
    def meter_a_phase_current(self, source='meter')->RegisterData:
        # A
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.APhaseCurrent))

    @property
    def meter_b_phase_current(self, source='meter')->RegisterData:
        # A
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.BPhaseCurrent))

    @property
    def meter_c_phase_current(self, source='meter')->RegisterData:
        # A
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.CPhaseCurrent))

    @property
    def meter_active_power(self, source='meter')->RegisterData:
        """
        >0: feed-in to the power grid.
        <0: supply from the power grid.
        """
        # W
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.ActivePower))

    @property
    def meter_reactive_power(self, source='meter')->RegisterData:
        # var
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.ReactivePower))

    @property
    def meter_power_factor(self, source='meter')->RegisterData:
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.PowerFactor))

    @property
    def meter_grid_frequency(self, source='meter')->RegisterData:
        # Hz
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.GridFrequency))

    @property
    def meter_positive_active_electricity(self, source='meter')->RegisterData:
        """
        Electricity fed by the inverter to the power grid.
        """
        # kWh
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.PositiveActiveElectricity))

    @property
    def meter_reverse_active_power(self, source='meter')->RegisterData:
        """
        Power supplied to a distributed system from the power grid.
        """
        # kWh
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.ReverseActivePower))

    @property
    def meter_meter_type(self, source='meter')->RegisterData:
        """
        0: single phase.
        1: three phase.
        """
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.MeterType))

    @property
    def meter_a_phase_active_power(self, source='meter')->RegisterData:
        """
        > 0: feed-in to the power grid.
        <0: supply from the power grid
        """
        # W
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.APhaseActivePower))

    @property
    def meter_b_phase_active_power(self, source='meter')->RegisterData:
        """
        > 0: feed-in to the power grid.
        <0: supply from the power grid
        """
        # W
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.BPhaseActivePower))

    @property
    def meter_c_phase_active_power(self, source='meter')->RegisterData:
        """
        > 0: feed-in to the power grid.
        <0: supply from the power grid
        """
        # W
        return RegisterData(source, self.read_data(registers.MeterEquipmentRegister.CPhaseActivePower))
