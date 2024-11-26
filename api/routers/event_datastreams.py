from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.database import db_service
from api.crud import crud_event_datastream


router = APIRouter()

@router.get("/api/event_datastreams")
def read_event_datastreams(
    event: str | None = None,
    db: Session = Depends((db_service.get_db))):
    """Get sensor boxes data for event monitoring.

    Args:
        event (str | None): Event name or None for all events
    """

    try:
        return crud_event_datastream.read_many(db, event)
    
    except Exception as e:
        raise e