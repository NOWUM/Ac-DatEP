from sqlalchemy import Column, String, Date, Text, Boolean, PrimaryKeyConstraint

from api.models.base import Base


class Event(Base):
    """
    Model representing an event.

    Attributes:
    - **event_name** (str): The name of the evet.
    - **date_from** (date): Start date of the event.
    - **date_to** (date): End date of the event.
    - **additional_info** (str, optional): Additional information for the event.
    - **confidential** (bool): A flag indicating whether the data is confidential. If set to True, the data should not be returned to all users.

    """
    __tablename__ = 'events'
    event_name = Column(String, primary_key=True)
    date_from = Column(Date)
    date_to = Column(Date)
    additional_info = Column(Text)
    confidential = Column(Boolean, nullable=False, default=False)


    __table_args__ = (
        {'extend_existing': True},
    )
