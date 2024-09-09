from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from api.models.base import Base


class Datastream(Base):
    """
    Model representing a datastream.

    Attributes:
    - **id** (int): The primary key ID of the datastream.
    - **sensor_id** (int): The foreign key ID referencing the associated sensor.
    - **ex_id** (int, optional): An external identifier for the datastream.
    - **type** (str): The type of datastream.
    - **unit** (str, optional): The unit of measurement for the datastream.
    - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data should not be returned to all users.

    - **sensor** (Sensor): Relationship to the associated sensor.
    """
    __tablename__ = 'datastreams'
    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer, ForeignKey('sensors.id'), nullable=False)
    ex_id = Column(Text)
    type = Column(Text, nullable=False)
    unit = Column(Text)
    confidential = Column(Boolean, nullable=False, default=True)

    sensor = relationship("Sensor")

    __table_args__ = (
        {'extend_existing': True},
    )
