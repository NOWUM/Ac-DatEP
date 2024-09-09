from typing import Optional

from pydantic import BaseModel

from datetime import date


class EventBase(BaseModel):
    """
    Base data model for an event.
    """

    event_name: str
    date_from: date
    date_to: date
    additional_info: Optional[str] = None
    confidential: bool = False

class EventCreate(EventBase):
    """
    Data model for creating an event.
    """
    pass

class EventUpdate(EventBase):
    """
    Data model for updating an event.
    """

class EventDB(EventBase):
    """
    Base data model for an event in the database.
    """
    class Config:
        from_attributes = True
