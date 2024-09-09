from .crud import CRUDBase
from api.models import Event
from api.schemas import EventCreate, EventUpdate


class CRUDEvent(CRUDBase[Event, EventCreate, EventUpdate]):
    pass

crud_event = CRUDEvent(Event)
