from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.schemas import EventDB, EventCreate, EventUpdate
from api.auth.dependencies import require_role
from api.database import db_service
from api.crud import crud_event

router = APIRouter()


@router.post(
    path="/api/events/",
    response_model=EventDB | List[EventDB] | None,
    dependencies=[Depends(require_role("admin"))])
def create_event(
    events: EventCreate | List[EventCreate],
    db: Session = Depends(db_service.get_db),
    on_duplicate: str = "raise"):

    try:
        return crud_event.create(
            db=db,
            obj_in=events,
            on_duplicate=on_duplicate)
    except HTTPException as e:
        raise e

@router.get(
        path="/api/events",
        response_model=List[EventDB])
def read_events(
    skip: int | None = None,
    limit: int | None = None,
    db: Session = Depends(db_service.get_db),
    is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a list of datastreams with optional pagination.

    Args:
    - **skip** (int, optional): The number of records to skip (for pagination). Defaults to 0.
    - **limit** (int, optional): The maximum number of records to return. Defaults to 100.

    Returns:
    - **List[Event]**: A list of event objects.
    """
    try:
        return crud_event.read_many(
            db=db,
            is_confidential_user=is_confidential_user,
            skip=skip,
            limit=limit)
    except HTTPException as e:
        raise e
