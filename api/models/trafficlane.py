from sqlalchemy import Column, String, Date, Text, Boolean, Integer

from api.models.base import Base


class Trafficlane(Base):
    """
    Model representing a trafficlane.

    Attributes:
    - **trafficlane_name** (str): The name of the evet.
    - **date_from** (date): Start date of the trafficlane.
    - **date_to** (date): End date of the trafficlane.
    - **additional_info** (str, optional): Additional information for the trafficlane.
    - **confidential** (bool): A flag indicating whether the data is confidential. If set to True, the data should not be returned to all users.

    """
    __tablename__ = 'trafficlanes'
    datastream_id = Column(Integer, primary_key=True)
    lane = Column(Text)
    speedlimit = Column(Integer)
    aggregation = Column(Text)
    confidential = Column(Boolean)

    __table_args__ = (
        {'extend_existing': True},
    )
