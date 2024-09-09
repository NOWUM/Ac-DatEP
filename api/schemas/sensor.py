from geoalchemy2.shape import to_shape
from pydantic import BaseModel, field_validator
from typing import Optional, Any, Union

from shapely.geometry.base import BaseGeometry


class SensorBase(BaseModel):
    """
    Base data model for a sensor.
    """
    source: str
    ex_id: Optional[str] = None
    description: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    geometry: Optional[Union[str, Any]] = None
    confidential: bool = True


class SensorCreate(SensorBase):
    """
    Data model for creating a sensor.
    """
    pass


class SensorUpdate(SensorBase):
    """
    Data model for updating a sensor.
    """
    pass


class SensorDB(SensorBase):
    """
    Base data model for a sensor in the database.
    """
    id: int

    @field_validator('geometry')
    @classmethod
    def set_geometry(cls, geometry):
        if geometry is not None:
            shape: BaseGeometry = to_shape(geometry)
            return shape.wkt
        return ''

    class Config:
        from_attributes = True
