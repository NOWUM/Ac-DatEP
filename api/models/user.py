from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Session

from api.models.base import Base


class User(Base):
    """
    Model representing a user.

    Attributes:
    - **id** (int): The primary key ID of the user.
    - **username** (str): The username of the user (unique).
    - **hashed_password** (str): The hashed password of the user.
    - **role** (str): The role of the user.
    """
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String)

    __table_args__ = (
        {'extend_existing': True},
    )


def get_user_by_username(username: str, db: Session):
    """
    Retrieve a user by username.

    Args:
    - **db** (Session): The database session.
    - **username** (str): The username of the user.

    Returns:
    - **User**: The user object with the specified username.
    """
    return db.query(User).filter_by(username=username).first()
