from fastapi import APIRouter, Depends
from api.database import db_service


router = APIRouter()

@router.get("/health")
def get_session():
    try:
        db_service.get_db()
        return True
    except Exception as e:
        return False

def is_database_online(session: bool = Depends(get_session)):
    return session


