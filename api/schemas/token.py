from uuid import UUID

from pydantic import BaseModel


class TokenBase(BaseModel):
    role: str
    institution: str


class TokenCreate(TokenBase):
    pass


class TokenUpdate(TokenBase):
    pass


class TokenDB(TokenBase):
    token: UUID

    class Config:
        from_attributes = True
