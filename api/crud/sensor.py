from .crud import CRUDBase
from api.schemas import SensorCreate, SensorUpdate
from api.models import Sensor


class CRUDSensor(CRUDBase[Sensor, SensorCreate, SensorUpdate]):
    pass


crud_sensor = CRUDSensor(Sensor)
