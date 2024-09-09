import re
from typing import Optional

from pydantic import BaseModel, field_validator
from pydantic_core.core_schema import FieldValidationInfo


def validate_password(password: str) -> str:
    """
    Validate the password based on certain criteria.

    Args:
    - **password** (str): The password to validate.

    Returns:
    - **str**: The validated password.

    Raises:
    - **ValueError**: If the password does not meet the validation criteria.
    """
    messages: [str] = []

    if not re.search("[a-z]", password):
        messages.append("Password must contain a lowercase letter: a-z")
    if not re.search("[A-Z]", password):
        messages.append("Password must contain an uppercase letter: A-Z")
    if not re.search("[0-9]", password):
        messages.append("Password must contain a digit: 0-9")
    if not re.search("[!@#$%^&*(),.?\":{}|<>]", password):
        allowed_special_characters = "!@#$%^&*(),.?\":{}|<>"
        messages.append(f"Password must contain a special character: {allowed_special_characters}")
    if len(messages):
        raise ValueError(' | '.join(messages))
    return password


class UserBase(BaseModel):
    """
    Base data model for a user.
    """
    username: str
    role: Optional[str] = "user"

    @field_validator('role')
    @classmethod
    def validate_role(cls, role):
        allowed_roles = ["user", "admin"]
        if role not in allowed_roles:
            raise ValueError(f"Role must be one of {allowed_roles}")
        return role


class UserCreate(UserBase):
    """
    Data model for creating a user.
    """
    username: str
    password: str
    role: Optional[str] = "user"

    @field_validator('password')
    @classmethod
    def validate_password(cls, password: str):
        return validate_password(password)


class UserUpdate(UserBase):
    """
    Data model for updating a user.
    """
    pass


class UserAuthenticate(BaseModel):
    """
    Data model for user authentication.
    """
    username: str
    password: str


class UserPasswordChange(BaseModel):
    """
    Data model for changing user password.
    """
    new_password: str
    new_password_repeat: str

    @field_validator('new_password')
    @classmethod
    def validate_password(cls, password: str):
        return validate_password(password)

    @field_validator('new_password_repeat')
    @classmethod
    def passwords_match(cls, password_repeat: str, info: FieldValidationInfo):
        if 'new_password' in info.data and password_repeat != info.data['new_password']:
            raise ValueError('Passwords do not match')
        return password_repeat


class UserDB(UserBase):
    """
    Base data model for a user in the database.
    """
    id: int

    class Config:
        from_attributes = True
