from pydantic import BaseModel
from datetime import datetime


class MeasurementBase(BaseModel):
    """
    Base data model for a measurement.
    """
    datastream_id: int
    timestamp: datetime
    value: float
    confidential: bool = True


class MeasurementCreate(MeasurementBase):
    """
    Data model for creating a measurement.
    """
    pass


class MeasurementUpdate(BaseModel):
    """
    Data model for updating a measurement.
    """
    value: float


class MeasurementDB(MeasurementBase):
    """
    Base data model for a measurement in the database.
    """

    class Config:
        from_attributes = True
