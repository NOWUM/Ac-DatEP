from typing import Optional

from pydantic import BaseModel

from datetime import date


class TrafficlaneBase(BaseModel):
    """
    Base data model for an trafficlane.
    """

    datastream_id: int
    lane: str
    aggregation: str
    speedlimit: Optional[int] = None
    confidential: Optional[bool] = False

class TrafficlaneCreate(TrafficlaneBase):
    """
    Data model for creating an trafficlane.
    """
    pass

class TrafficlaneUpdate(TrafficlaneBase):
    """
    Data model for updating an trafficlane.
    """

class TrafficlaneDB(TrafficlaneBase):
    """
    Base data model for an trafficlane in the database.
    """
    class Config:
        from_attributes = True
