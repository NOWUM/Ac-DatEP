from sqlalchemy import Column, Integer, Float, Text, func, Boolean
from geoalchemy2 import Geometry

from api.models.base import Base


class Sensor(Base):
    """
    Model representing a sensor.

    Attributes:
    - **id** (int): The primary key ID of the sensor.
    - **source** (str): The source of the sensor data.
    - **ex_id** (int, optional): An external identifier for the sensor.
    - **description** (str, optional): A description of the sensor.
    - **geometry** (Geometry): The geometry of the sensor (GeoAlchemy2 type).
    - **longitude** (float): The longitude coordinate of the sensor.
    - **latitude** (float): The latitude coordinate of the sensor.
    - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data should not be returned to all users.
    """
    __tablename__ = 'sensors'
    id = Column(Integer, primary_key=True)
    source = Column(Text, nullable=False)
    ex_id = Column(Text, index=True)
    description = Column(Text)
    geometry = Column(Geometry(geometry_type='GEOMETRY', srid=25832), nullable=False)
    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)
    confidential = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        {'extend_existing': True},
    )

    def set_geometry(self):
        """
        Set the geometry of the sensor based on longitude and latitude coordinates.
        """
        self.geometry = func.ST_GeomFromText(f'POINT({self.longitude} {self.latitude})', 25832)
