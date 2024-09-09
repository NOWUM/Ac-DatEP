from .crud import CRUDBase
from api.models import Trafficlane
from api.schemas import TrafficlaneCreate, TrafficlaneUpdate


class CRUDTrafficlane(CRUDBase[Trafficlane, TrafficlaneCreate, TrafficlaneUpdate]):
    pass

crud_trafficlane = CRUDTrafficlane(Trafficlane)
