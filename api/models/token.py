from sqlalchemy import Column, Text, UUID

from api.models.base import Base


class Token(Base):
    __tablename__ = 'tokens'
    id = Column(UUID(as_uuid=True), primary_key=True)
    role = Column(Text, nullable=False)
    institution = Column(Text, nullable=False)

    __table_args__ = (
        {'extend_existing': True},
    )
