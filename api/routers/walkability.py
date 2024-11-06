from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.database import db_service
from api.crud import crud_walkability

from typing import List

router = APIRouter()

@router.get("/api/walkability")
def read_walkability(
    persona: str,
    db: Session = Depends((db_service.get_db))):
    """Get walkability data for specific persona for the city of Aachen, Germany.

    Args:
    - **persona (str)**: Persona to get walkability data for, valid values are 'student', 'senior' or 'family'

    """

    try:
        return crud_walkability.read_many(db, persona)
    
    except Exception as e:
        raise e