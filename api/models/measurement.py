from sqlalchemy import Column, Integer, Float, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from api.models.base import Base


class Measurement(Base):
    """
    Model representing a measurement.

    Attributes:
    - **datastream_id** (int): The foreign key ID referencing the associated datastream.
    - **timestamp** (DateTime): The timestamp of the measurement.
    - **value** (float): The value of the measurement.
    - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data should not be returned to all users.

    - **datastream** (Datastream): Relationship to the associated datastream.
    """
    __tablename__ = 'measurements'
    datastream_id = Column(Integer, ForeignKey('datastreams.id'), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    value = Column(Float, nullable=False)
    confidential = Column(Boolean, nullable=False, default=True)

    datastream = relationship("Datastream")

    __table_args__ = (
        {'extend_existing': True},
    )
