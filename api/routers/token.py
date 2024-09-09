import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from api.schemas import TokenDB, TokenCreate
from api.crud import crud_token
from api.database import db_service

router = APIRouter()


@router.post("/api/tokens/", response_model=TokenDB)
def create_token(tokens: TokenCreate, db: Session = Depends(db_service.get_db)):
    return crud_token.create(db=db, obj_in=tokens)


@router.get("/api/tokens/", response_model=List[TokenDB])
def read_tokens(skip: int = 0, limit: int = 100, db: Session = Depends(db_service.get_db)):
    tokens = crud_token.get_all(db, skip=skip, limit=limit)
    return tokens


@router.delete("/api/tokens/{token}", response_model=TokenDB)
def delete_token(token: uuid.UUID, db: Session = Depends(db_service.get_db)):
    db_sensor = crud_token.get(db, token=token)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Token not found")
    return crud_token.delete(db=db, token=token)
