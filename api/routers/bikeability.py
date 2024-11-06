from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.database import db_service
from api.crud import crud_bikeability


router = APIRouter()

@router.get("/api/bikeability")
def read_bikeability(db: Session = Depends((db_service.get_db))):
    """Get bikeability data for the city of Aachen, Germany."""

    try:
        return crud_bikeability.read_many(db)
    
    except Exception as e:
        raise e