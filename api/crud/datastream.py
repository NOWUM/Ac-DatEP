from .crud import CRUDBase
from api.models import Datastream
from api.schemas import DatastreamCreate, DatastreamUpdate


class CRUDDatastream(CRUDBase[Datastream, DatastreamCreate, DatastreamUpdate]):
    pass


crud_datastream = CRUDDatastream(Datastream)
