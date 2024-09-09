from typing import Optional

from pydantic import BaseModel


class DatastreamBase(BaseModel):
    """
    Base data model for a datastream.
    """
    sensor_id: int
    ex_id: Optional[int] = None
    type: str
    unit: str
    confidential: bool = True


class DatastreamCreate(DatastreamBase):
    """
    Data model for creating a datastream.
    """
    pass


class DatastreamUpdate(DatastreamBase):
    """
    Data model for updating a datastream.
    """
    pass


class DatastreamDB(DatastreamBase):
    """
    Base data model for a datastream in the database.
    """
    id: int

    class Config:
        from_attributes = True
