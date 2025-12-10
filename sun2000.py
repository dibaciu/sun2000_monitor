import logging
from dataclasses import dataclass
from typing import Union, Dict

from sun2000_modbus import inverter
from sun2000_modbus import registers

from config import MonitorConfig

logger = logging.getLogger(__name__)

@dataclass()
class RegisterData:
    source: str
    value: Union[str, int, float, None]

class Sun2000:
    def __init__(self, config:MonitorConfig)->None:
        self.config = config
        self.inverter = inverter.Sun2000(host=config.sun2000_inverter_host, port=config.sun2000_inverter_port)
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

    def ping(self)->bool:
        self.inverter.connect()
        return self.inverter.isConnected()

    def read_data(self, register:Union[registers.InverterEquipmentRegister, registers.BatteryEquipmentRegister, registers.MeterEquipmentRegister], read_formatted:bool=False)->Union[str, int, float, None]:
        if not self.inverter.isConnected():
            self.inverter.connect()
        if read_formatted:
            data = self.inverter.read_formatted(register=register)
        else:
            data = self.inverter.read(register=register)
        return data

    def poll_all(self):
        return dict([(register, getattr(self, register)) for register in self.registers_to_poll])

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
        state1_mapping = {
            '0': 'Standby',
            '1': 'Grid connected',
            '2': 'Grid connected',
            '3': 'Grid connection with derating due to power rationing',
            '4': 'Grid connection with derating due to internal causes of the solar inverter',
            '5': 'Normal stop',
            '6': 'Stop due to faults',
            '7': 'Stop due to power rationing',
            '8': 'Shutdown',
            '9': 'Spot check',
        }
        state1_u16 = self.read_data(registers.InverterEquipmentRegister.State1)
        try:
            state1 = [state1_mapping[str(len(state1_u16) - 1 - i)] for i in range(len(state1_u16) ) if state1_u16[i] == '1' and str(len(state1_u16) - 1 - i) in state1_mapping][0]
        except IndexError:
            state1 = 'Unknown'
        return RegisterData(source, state1)

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
