import dynamixel_sdk as dxl
from threading import Lock
import inspect

# These codes are available in the EEPROM Control Table listings for
# each device at https://emanual.robotis.com/docs/en/software/dynamixel/dynamixel_workbench/#supported-dynamixel

MODEL_NUMBER_AX12A = 12
MODEL_NUMBER_AX12W = 300
MODEL_NUMBER_AX18A = 18
MODEL_NUMBER_EX106 = 107
MODEL_NUMBER_RX10 = 10
MODEL_NUMBER_RX24F = 24
MODEL_NUMBER_RX28 = 28
MODEL_NUMBER_RX64 = 64
MODEL_NUMBER_MX12W = 360
MODEL_NUMBER_MX28 = 29
MODEL_NUMBER_MX64 = 310
MODEL_NUMBER_MX106 = 320
MODEL_NUMBER_MX28_2 = 30
MODEL_NUMBER_MX64_2 = 311
MODEL_NUMBER_MX106_2 = 321

g_model_str_to_code_dict = {
    'AX12A': MODEL_NUMBER_AX12A,
    'AX12W': MODEL_NUMBER_AX12W,
    'AX18A': MODEL_NUMBER_AX18A,
    'EX106': MODEL_NUMBER_EX106,
    'RX10': MODEL_NUMBER_RX10,
    'RX24F': MODEL_NUMBER_RX24F,
    'RX28': MODEL_NUMBER_RX28,
    'RX64': MODEL_NUMBER_RX64,
    'MX12W': MODEL_NUMBER_MX12W,
    'MX28': MODEL_NUMBER_MX28,
    'MX64': MODEL_NUMBER_MX64,
    'MX106': MODEL_NUMBER_MX106,
    'MX28_2': MODEL_NUMBER_MX28_2,
    'MX64_2': MODEL_NUMBER_MX64_2,
    'MX106_2': MODEL_NUMBER_MX106_2,
}

g_model_code_to_str_dict = {
    MODEL_NUMBER_AX12A: 'AX12A',
    MODEL_NUMBER_AX12W: 'AX12W',
    MODEL_NUMBER_AX18A: 'AX18A',
    MODEL_NUMBER_RX10: 'RX10',
    MODEL_NUMBER_EX106: 'EX106',
    MODEL_NUMBER_RX24F: 'RX24F',
    MODEL_NUMBER_RX28: 'RX28',
    MODEL_NUMBER_RX64: 'RX64',
    MODEL_NUMBER_MX12W: 'MX12W',
    MODEL_NUMBER_MX28: 'MX28',
    MODEL_NUMBER_MX64: 'MX64',
    MODEL_NUMBER_MX106: 'MX106',
    MODEL_NUMBER_MX28_2: 'MX28_2',
    MODEL_NUMBER_MX64_2: 'MX64_2',
    MODEL_NUMBER_MX106_2: 'MX106_2',
}

EEPROM_FIRMWARE_VERSION = 2
EEPROM_RETURN_DELAY_TIME = 5
EEPROM_CW_ANGLE_LIMIT = 6
EEPROM_CCW_ANGLE_LIMIT = 8
EEPROM_TEMPERATURE_LIMIT = 11
EEPROM_MIN_VOLTAGE_LIMIT = 12
EEPROM_MAX_VOLTAGE_LIMIT = 13
EEPROM_MAX_TORQUE = 14
EEPROM_STATUS_RETURN_LEVEL = 16
EEPROM_ALARM_LED = 17
EEPROM_SHUTDOWN = 18

MX_EEPROM_MULTI_TURN_OFFSET = 20
MX_EEPROM_RESOLUTION_DIVIDER = 22

RAM_TORQUE_ENABLE = 24
RAM_LED = 25
RAM_GOAL_POSITION = 30
RAM_MOVING_SPEED = 32
RAM_TORQUE_LIMIT = 34
RAM_PRESENT_POSITION = 36
RAM_PRESENT_SPEED = 38
RAM_PRESENT_LOAD = 40
RAM_PRESENT_VOLTAGE = 42
RAM_PRESENT_TEMPERATURE = 43
RAM_REGISTERED = 44
RAM_MOVING = 46
RAM_LOCK = 47
RAM_PUNCH = 48

AX_RAM_CW_COMPLIANCE_MARGIN = 26
AX_RAM_CCW_COMPLIANCE_MARGIN = 27
AX_RAM_CW_COMPLIANCE_SLOPE = 28
AX_RAM_CCW_COMPLIANCE_SLOPE = 29

EX_RAM_SENSED_CURRENT = 56

MX_RAM_D_GAIN = 26
MX_RAM_I_GAIN = 27
MX_RAM_P_GAIN = 28
MX_RAM_REALTIME_TICK = 50
MX_RAM_GOAL_ACCELERATION = 73

MX64_RAM_CURRENT = 68
MX64_RAM_TORQUE_CTRL_MODE_ENABLE = 70
MX64_RAM_GOAL_TORQUE = 71

# Flags for shutdown and alarmLED registers
ERROR_BIT_VOLTAGE = 1
ERROR_BIT_ANGLE_LIMIt = 2
ERROR_BIT_OVERHEATING = 4
ERROR_BIT_RANGE = 8
ERROR_BIT_CHECKSUM = 16
ERROR_BIT_OVERLOAD = 32
ERROR_BIT_INSTRUCTION = 64

class DXLException(Exception):
    def __init__(self, msg):
        self.msg = msg
        frame = inspect.stack()[1][0]
        info = inspect.getframeinfo(frame)
        self.file = info.filename
        self.function = info.function
        self.line = info.lineno

    def __str__(self):
        return f"DXL Exception caught on line {self.line} of {self.file}: {self.msg}"

class DXLPort:
    def __init__(self, address: str, baud: int = 57600):
        self.lock = Lock()
        self.baud = baud
        self.port = None
        port = dxl.PortHandler(address)
        self.actuators = {}
        self.syncRegister = None
        self.syncLock = Lock()
        self.syncEncoder = None
        self.syncReadDXLs = None
        self.syncWriteCount = 0

        try:
            port.openPort()
        except Exception as x:
            raise(DXLException(str(x)))

        self.port = port

        try:
            self.port.setBaudRate(self.baud)
        except Exception as x:
            raise(DXLException(str(x)))

        self.packetHandler = dxl.PacketHandler(1.0)

    def __del__(self):
        if self.port != None:
            with self.lock:
                self.port.closePort()

    def scan(self):
        for i in range(254):
            print(f'Pinging {i}')
            actuator = self.getDXL(i)
            if actuator != None:
                print(f'Found: {i}')
                self.actuators[i] = actuator

    def getDXL(self, id: int):
        if id in self.actuators.keys():
            dxl = self.actuators[id]
            return dxl

        model, result, error = self.ping(id)
        if model in [MODEL_NUMBER_AX12A, MODEL_NUMBER_AX12W, MODEL_NUMBER_AX18A, MODEL_NUMBER_RX10, MODEL_NUMBER_RX24F, MODEL_NUMBER_RX28, MODEL_NUMBER_RX64]:
            return DXL_AX(self, id, model)
        elif model == MODEL_NUMBER_EX106:
            return DXL_EX(self, id, model)
        elif model in [MODEL_NUMBER_MX12W, MODEL_NUMBER_MX28]:
            return DXL_MX(self, id, model)
        elif model in [MODEL_NUMBER_MX64, MODEL_NUMBER_MX106]:
            return DXL_MX64(self, id, model)
        else:
            return None

    def recover(self, id: int):
        with self.syncLock:
            self.writeUInt8(id, RAM_TORQUE_ENABLE, 0)
            time.sleep(1.0)
            self.writeUInt16(id, RAM_GOAL_POSITION, self.readUInt16(id, RAM_PRESENT_POSITION))
            self.writeUInt8(id, RAM_TORQUE_LIMIT, 1024)
            self.writeUInt8(id, RAM_TORQUE_ENABLE, 1)

    @staticmethod
    def modelName(code: int):
        return g_model_code_to_str_dict[code]

    @staticmethod
    def modelCode(name: str):
        return g_model_str_to_code_dict[name]

    def resultString(self, result: int):
        return self.packetHandler.getTxRxResult(result)

    def errorString(self, error: int):
        return self.packetHandler.getTxRxPacketEror(error)

    def ping(self, id: int):
        with self.lock:
            model, result, error = self.packetHandler.ping(self.port, id)
        return model, result, error

    def readUInt8(self, id: int, register: int):
        with self.lock:
            value, result, error = self.packetHandler.read1ByteTxRx(self.port, id, register)
        return value, result, error

    def readUInt16(self, id: int, register: int):
        with self.lock:
            value, result, error = self.packetHandler.read2ByteTxRx(self.port, id, register)
        return value, result, error

    def writeUInt8(self, id: int, register: int, value: int):
        with self.lock:
            result, error = self.packetHandler.write1ByteTxRx(self.port, id, register, value)
        return result, error

    def writeUInt16(self, id: int, register: int, value: int):
        with self.lock:
            result, error = self.packetHandler.write2ByteTxRx(self.port, id, register, value)
        return result, error

    def syncReadInit(self, register: int, dataLen: int):
        self.syncLock.acquire()
        self.syncEncoder = dxl.GroupSyncRead(self.port, self.packetHandler, register, dataLen)
        self.syncRegister = register
        self.syncReadDXLs = []

    def syncReadPush(self, dxl, register: int):
        if self.syncRegister == register:
            if self.syncEncoder.addParam(id):
                self.syncReadDXLs.append(dxl)
                return True
        return False

    def syncReadComplete(self):
        self.syncRegister = None
        result = dxl.COMM_SUCCESS
        data = {}
        if len(syncReadhDXLs) > 0:
            with self.lock():
                result = self.syncEncoder.txRxPacket()
            data = { dxl.id: dxl.convert(self.register, syncEncoder.getData(dxl.id)) for dxl in self.syncReadDXLs }
        self.syncEncoder = None
        self.syncReadDXLs = None
        self.syncLock.release()
        return result, data

    def syncWriteInit(self, register: int, dataLen: int):
        self.syncLock.acquire()
        self.syncEncoder = dxl.GroupSyncWrite(self.port, self.packetHandler, register, dataLen)
        self.syncRegister = register
        self.syncWriteCount = 0

    def syncWritePush(self, dxl, register: int, value: int):
        if self.syncRegister == register:
            param = [DXL_LOBYTE(DXL_LOWORD(value)), DXL_HIBYTE(DXL_LOWORD(value)), DXL_LOBYTE(DXL_HIWORD(value)), DXL_HIBYTE(DXL_HIWORD(value))]
            if not self.syncEncoder.addParam(dxl.id, param):
                return False
            self.syncWriteCount += 1
            return True
        return False

    def syncWriteComplete(self):
        self.syncRegister = None
        result = dxl.COMM_SUCCESS
        if self.syncWriteCount > 0:
            with self.lock():
                result = self.syncEncoder.txPacket()
        self.syncEncoder = None
        self.syncLock.release()
        return result

class DXL:
    def __init__(self, port: DXLPort, id: int, model: int):
        self.port = port
        self.id = id
        self.model = model
        self.result = None
        self.error = None
        self.offset = 0.0

    def convert(self, register: int, value: int):
        if register in [RAM_GOAL_POSITION, RAM_PRESENT_POSITION, EEPROM_CW_ANGLE_LIMIT, EEPROM_CCW_ANGLE_LIMIT]:
            return self.toDegrees(value)
        return value

    @property
    def stepResolution(self):
        raise DXLException("stepResolution property needs to be overridden in the base classs")
        return 0.0

    @property
    def centerOffset(self):
        raise DXLException("centerOffset property needs to be overridden in the base classs")
        return 0

    def recover(self):
        self.port.recover(self.id)

    def fromDegrees(self, degrees: float)->int:
        return self.centerOffset + (degrees + self.offset) / self.stepResolution

    def toDegrees(self, steps: int)->float:
        return (steps - self.centerOffset) * step.stepResolution - self.offset

    def ping(self)->(int, int, int):
        return self.port.ping(self.id)

    @property
    def firmwareVersion(self):
        if self.port.syncReadPush(self, EEPROM_FIRMWARE_VERSION): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_FIRMWARE_VERSION)
        return value

    @property
    def returnDelayTime(self): # In microseconds
        if self.port.syncReadPush(self, EEPROM_RETURN_DELAY_TIME): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_RETURN_DELAY_TIME)
        return value << 1

    @returnDelayTime.setter
    def returnDelayTime(self, value: int): # In microseconds
        if self.port.syncWritePush(self, EEPROM_RETURN_DELAY_TIME, value >> 1): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_RETURN_DELAY_TIME, value >> 1)

    @property
    def cwAngleLimit(self):
        if self.port.syncReadPush(self, EEPROM_CW_ANGLE_LIMIT): return None
        steps, self.result, self.error = self.port.readUInt16(self.id, EEPROM_CW_ANGLE_LIMIT)
        return self.toDegrees(steps)

    @cwAngleLimit.setter
    def cwAngleLimit(self, value: int):
        steps = self.fromDegrees(value)
        if self.port.syncWritePush(self, EEPROM_CW_ANGLE_LIMIT, steps): return
        self.result, self.error = self.port.writeUInt16(self.id, EEPROM_CW_ANGLE_LIMIT, steps)

    @property
    def ccwAngleLimit(self):
        if self.port.syncReadPush(self, EEPROM_CCW_ANGLE_LIMIT): return None
        steps, self.result, self.error = self.port.readUInt16(self.id, EEPROM_CCW_ANGLE_LIMIT)
        return self.toDegrees(steps)

    @ccwAngleLimit.setter
    def ccwAngleLimit(self, value: int):
        steps = self.fromDegrees(value)
        if self.port.syncWritePush(self, EEPROM_CCW_ANGLE_LIMIT, steps): return
        self.result, self.error = self.port.writeUInt16(self.id, EEPROM_CCW_ANGLE_LIMIT, steps)

    @property
    def temperatureLimit(self): # In degrees
        if self.port.syncReadPush(self, EEPROM_TEMPERATURE_LIMIT): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_TEMPERATURE_LIMIT)
        return value

    '''
    # Disabled. It is inadvisable to set the temperature limit to anything other
    # than that set by the factory
    @temperatureLimit.setter
    def temperatureLimit(self, value: int):
        if self.port.syncWritePush(self, EEPROM_TEMPERATURE_LIMIT, value):
            return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_TEMPERATURE_LIMIT, value)
    '''

    @property
    def minVoltageLimit(self): # In volts
        if self.port.syncReadPush(self, EEPROM_MIN_VOLTAGE_LIMIT): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_MIN_VOLTAGE_LIMIT)
        return float(value) * 0.1

    @minVoltageLimit.setter
    def minVoltageLimit(self, volts: float): # In volts
        value = int(volts * 10.0)
        if value < 50:
            value = 50
        elif value > 160:
            value = 160
        if self.port.syncWritePush(self, EEPROM_MIN_VOLTAGE_LIMIT, value): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_MIN_VOLTAGE_LIMIT, int(value * 10.0))

    @property
    def maxVoltageLimit(self): # In volts
        if self.port.syncReadPush(self, EEPROM_MAX_VOLTAGE_LIMIT): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_MAX_VOLTAGE_LIMIT)
        return float(value) * 0.1

    @maxVoltageLimit.setter
    def maxVoltageLimit(self, volts: int): # In volts
        value = int(volts * 10.0)
        if value < 50:
            value = 50
        elif value > 160:
            value = 160
        if self.port.syncWritePush(self, EEPROM_MAX_VOLTAGE_LIMIT, value): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_MAX_VOLTAGE_LIMIT, value)

    @property
    def maxTorque(self):
        if self.port.syncReadPush(self, EEPROM_MAX_TORQUE): return None
        value, self.result, self.error = self.port.readUInt16(self.id, EEPROM_MAX_TORQUE)
        return value

    @maxTorque.setter
    def maxTorque(self, value: int):
        if self.port.syncWritePush(self, EEPROM_MAX_TORQUE, value): return
        self.result, self.error = self.port.writeUInt16(self.id, EEPROM_MAX_TORQUE, value)

    # 0 = return status packet for ping instruction only
    # 1 = return status packet for ping and read instructions only
    # 2 = return status packet for all instructions
    @property
    def statusReturnLevel(self):
        if self.port.syncReadPush(self, EEPROM_STATUS_RETURN_LEVEL): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_STATUS_RETURN_LEVEL)
        return value

    @statusReturnLevel.setter
    def statusReturnLevel(self, value: int):
        if self.port.syncWritePush(self, EEPROM_STATUS_RETURN_LEVEL, value): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_STATUS_RETURN_LEVEL, value)

    @property
    def shutdown(self):
        if self.port.syncReadPush(self, EEPROM_SHUTDOWN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_SHUTDOWN)
        return value

    @shutdown.setter
    def shutdown(self, value: int):
        if self.port.syncWritePush(self, EEPROM_SHUTDOWN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_SHUTDOWN, value)

    @property
    def alarmLED(self):
        if self.port.syncReadPush(self, EEPROM_ALARM_LED): return None
        value, self.result, self.error = self.port.readUInt8(self.id, EEPROM_ALARM_LED)
        return value

    @alarmLED.setter
    def alarmLED(self, value: int):
        if self.port.syncWritePush(self, EEPROM_ALARM_LED, value): return
        self.result, self.error = self.port.writeUInt8(self.id, EEPROM_ALARM_LED, value)

    @property
    def torqueEnable(self):
        if self.port.syncReadPush(self, RAM_TORQUE_ENABLE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_TORQUE_ENABLE)
        return value

    @torqueEnable.setter
    def torqueEnable(self, value: int):
        if self.port.syncWritePush(self, RAM_TORQUE_ENABLE, value): return
        self.result, self.error = self.port.writeUInt8(self.id, RAM_TORQUE_ENABLE, value)

    @property
    def led(self):
        if self.port.syncReadPush(self, RAM_LED): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_LED)
        return value

    @led.setter
    def led(self, value: int):
        if self.port.syncWritePush(self, RAM_LED, value): return
        self.result, self.error = self.port.writeUInt8(self.id, RAM_LED, value)

    @property
    def goalPosition(self):
        if self.port.syncReadPush(self, RAM_GOAL_POSITION): return None
        steps, self.result, self.error = self.port.readUInt16(self.id, RAM_GOAL_POSITION)
        return self.toDegrees(steps)

    @goalPosition.setter
    def goalPosition(self, value: int):
        steps = self.fromDegrees(value)
        if self.port.syncWritePush(self, RAM_GOAL_POSITION, steps): return
        self.result, self.error = self.port.writeUInt16(self.id, RAM_GOAL_POSITION, steps)

    @property
    def movingSpeed(self):
        if self.port.syncReadPush(self, RAM_MOVING_SPEED): return None
        value, self.result, self.error = self.port.readUInt16(self.id, RAM_MOVING_SPEED)
        return value

    @movingSpeed.setter
    def movingSpeed(self, value: int):
        if self.port.syncWritePush(self, RAM_MOVING_SPEED, value): return
        self.result, self.error = self.port.writeUInt16(self.id, RAM_MOVING_SPEED, value)

    @property
    def torqueLimit(self):
        if self.port.syncReadPush(self, RAM_TORQUE_LIMIT): return None
        value, self.result, self.error = self.port.readUInt16(self.id, RAM_TORQUE_LIMIT)
        return value

    @torqueLimit.setter
    def torqueLimit(self, value: int):
        if self.port.syncWritePush(self, RAM_TORQUE_LIMIT, value): return
        self.result, self.error = self.port.writeUInt16(self.id, RAM_TORQUE_LIMIT, value)

    @property
    def presentPosition(self):
        if self.port.syncReadPush(self, RAM_PRESENT_POSITION): return None
        steps, self.result, self.error = self.port.readUInt16(self.id, RAM_PRESENT_POSITION)
        return self.toDegrees(self)

    @property
    def presentSpeed(self):
        if self.port.syncReadPush(self, RAM_PRESENT_SPEED): return None
        value, self.result, self.error = self.port.readUInt16(self.id, RAM_PRESENT_SPEED)
        return value

    @property
    def presentLoad(self):
        if self.port.syncReadPush(self, RAM_PRESENT_LOAD): return None
        value, self.result, self.error = self.port.readUInt16(self.id, RAM_PRESENT_LOAD)
        return value

    @property
    def presentVoltage(self):
        if self.port.syncReadPush(self, RAM_PRESENT_VOLTAGE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_PRESENT_VOLTAGE)
        return value

    @property
    def presentTemperature(self):
        if self.port.syncReadPush(self, RAM_PRESENT_TEMPERATURE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_PRESENT_TEMPERATURE)
        return value

    @property
    def registered(self):
        if self.port.syncReadPush(self, RAM_REGISTERED): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_REGISTERED)
        return value

    @property
    def moving(self):
        if self.port.syncReadPush(self, RAM_MOVING): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_MOVING)
        return value

    @property
    def lock(self):
        if self.port.syncReadPush(self, RAM_LOCK): return None
        value, self.result, self.error = self.port.readUInt8(self.id, RAM_LOCK)
        return value

    @lock.setter
    def lock(self, value: int):
        if self.port.syncWritePush(self, RAM_LOCK, value): return
        self.result, self.error = self.port.writeUInt8(self.id, RAM_LOCK, value)

    @property
    def punch(self):
        if self.port.syncReadPush(self, RAM_PUNCH): return None
        value, self.result, self.error = self.port.readUInt16(self.id, RAM_PUNCH)
        return value

    @punch.setter
    def punch(self, value: int):
        if self.port.syncWritePush(self, RAM_PUNCH, value): return
        self.result, self.error = self.port.writeUInt16(self.id, RAM_PUNCH, value)

class DXL_AX(DXL):
    @DXL.stepResolution.getter # Python's oop implementation is so fucking weird
    def stepResolution(self):
        return 0.29

    @DXL.centerOffset.getter
    def centerOffset(self):
        return 512

    @property
    def cwComplianceMargin(self):
        if self.port.syncReadPush(self, AX_RAM_CW_COMPLIANCE_MARGIN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, AX_RAM_CW_COMPLIANCE_MARGIN)
        return value

    @cwComplianceMargin.setter
    def cwComplianceMargin(self, value: int):
        if self.port.syncWritePush(self, AX_RAM_CW_COMPLIANCE_MARGIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, AX_RAM_CW_COMPLIANCE_MARGIN, value)

    @property
    def ccwComplianceMargin(self):
        if self.port.syncReadPush(self, AX_RAM_CCW_COMPLIANCE_MARGIN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, AX_RAM_CCW_COMPLIANCE_MARGIN)

    @ccwComplianceMargin.setter
    def ccwComplianceMargin(self, value: int):
        if self.port.syncWritePush(self, AX_RAM_CCW_COMPLIANCE_MARGIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, AX_RAM_CCW_COMPLIANCE_MARGIN, value)

    @property
    def cwComplianceSlope(self):
        if self.port.syncReadPush(self, AX_RAM_CW_COMPLIANCE_SLOPE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, AX_RAM_CW_COMPLIANCE_SLOPE)
        return value

    @cwComplianceSlope.setter
    def cwComplianceSlope(self, value: int):
        if self.port.syncWritePush(self, AX_RAM_CW_COMPLIANCE_MARGIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, AX_RAM_CW_COMPLIANCE_MARGIN, SLOPE)

    @property
    def ccwComplianceSlope(self):
        if self.port.syncReadPush(self, AX_RAM_CCW_COMPLIANCE_SLOPE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, AX_RAM_CCW_COMPLIANCE_SLOPE)
        return value

    @ccwComplianceSlope.setter
    def ccwComplianceSlope(self, value: int):
        if self.port.syncWritePush(self, AX_RAM_CCW_COMPLIANCE_SLOPE, value): return
        self.result, self.error = self.port.writeUInt8(self.id, AX_RAM_CCW_COMPLIANCE_SLOPE, value)

class DXL_EX(DXL_AX):
    @DXL_AX.stepResolution.getter
    def stepResolution(self):
        return 0.06127

    @DXL_AX.centerOffset.getter
    def centerOffset(self):
        return 2048

    @property
    def sensedCurrent(self):
        if self.port.syncReadPush(self, EX_RAM_SENSED_CURRENT): return None
        value, self.result, self.error = self.port.readUInt16(self.id, EX_RAM_SENSED_CURRENT)
        return value

class DXL_MX(DXL):
    @DXL.stepResolution.getter
    def stepResolution(self):
        return 0.088

    @DXL.centerOffset.getter
    def centerOffset(self):
        return 2048

    @property
    def multiTurnOffset(self):
        if self.port.syncReadPush(self, MX_EEPROM_MULTI_TURN_OFFSET): return None
        value, self.result, self.error = self.port.readUInt16(self.id, MX_EEPROM_MULTI_TURN_OFFSET)
        return value

    @multiTurnOffset.setter
    def multiTurnOffset(self, value: int):
        if self.port.syncWritePush(self, MX_EEPROM_MULTI_TURN_OFFSET, value): return
        self.result, self.error = self.port.writeUInt16(self.id, MX_EEPROM_MULTI_TURN_OFFSET, value)

    @property
    def resolutionDivider(self):
        if self.port.syncReadPush(self, MX_EEPROM_RESOLUTION_DIVIDER): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX_EEPROM_RESOLUTION_DIVIDER)
        return value

    @resolutionDivider.setter
    def resolutionDivider(self, value: int):
        if self.port.syncWritePush(self, MX_EEPROM_RESOLUTION_DIVIDER, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX_EEPROM_RESOLUTION_DIVIDER, value)

    @property
    def dGain(self):
        if self.port.syncReadPush(self, MX_RAM_D_GAIN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX_RAM_D_GAIN)
        return value

    @dGain.setter
    def dGain(self, value: int):
        if self.port.syncWritePush(self, MX_RAM_D_GAIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX_RAM_D_GAIN, value)

    @property
    def iGain(self):
        if self.port.syncReadPush(self, MX_RAM_I_GAIN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX_RAM_I_GAIN)
        return value

    @iGain.setter
    def iGain(self, value: int):
        if self.port.syncWritePush(self, MX_RAM_I_GAIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX_RAM_I_GAIN, value)

    @property
    def pGain(self):
        if self.port.syncReadPush(self, MX_RAM_P_GAIN): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX_RAM_P_GAIN)
        return value

    @pGain.setter
    def pGain(self, value: int):
        if self.port.syncWritePush(self, MX_RAM_P_GAIN, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX_RAM_P_GAIN, value)

    @property
    def realtimeTick(self):
        if self.port.syncReadPush(self, MX_RAM_REALTIME_TICK): return None
        value, self.result, self.error = self.port.readUInt16(self.id, MX_RAM_REALTIME_TICK)
        return value

    @property
    def goalAcceleration(self):
        if self.port.syncReadPush(self, MX_RAM_GOAL_ACCELERATION): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX_RAM_GOAL_ACCELERATION)
        return value

    @goalAcceleration.setter
    def goalAcceleration(self, value: int):
        if self.port.syncWritePush(self, MX_RAM_GOAL_ACCELERATION, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX_RAM_GOAL_ACCELERATION, value)

class DXL_MX64(DXL_MX):
    @property
    def current(self):
        if self.port.syncReadPush(self, MX64_RAM_CURRENT): return None
        value, self.result, self.error = self.port.readUInt16(self.id, MX64_RAM_CURRENT)
        return value

    @current.setter
    def current(self, value: int):
        if self.port.syncWritePush(self, MX64_RAM_CURRENT, value): return
        self.result, self.error = self.port.writeUInt16(self.id, MX64_RAM_CURRENT, value)

    @property
    def torqueCtlModeEnable(self):
        if self.port.syncReadPush(self, MX64_RAM_TORQUE_CTL_MODE_ENABLE): return None
        value, self.result, self.error = self.port.readUInt8(self.id, MX64_RAM_TORQUE_CTL_MODE_ENABLE)
        return value

    @torqueCtlModeEnable.setter
    def torqueCtlModeEnable(self, value: int):
        if self.port.syncWritePush(self, MX64_RAM_TORQUE_CTL_MODE_ENABLE, value): return
        self.result, self.error = self.port.writeUInt8(self.id, MX64_RAM_TORQUE_CTL_MODE_ENABLE, value)

    @property
    def goalTorque(self):
        if self.port.syncReadPush(self, MX64_RAM_GOAL_TORQUE): return None
        value, self.result, self.error = self.port.readUInt16(self.id, MX64_RAM_GOAL_TORQUE)
        return value

    @current.setter
    def goalTorque(self, value: int):
        if self.port.syncWritePush(self, MX64_RAM_GOAL_TORQUE, value): return
        self.result, self.error = self.port.writeUInt16(self.id, MX64_RAM_GOAL_TORQUE, value)
