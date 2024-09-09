from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from api.schemas import TrafficlaneDB, TrafficlaneCreate, TrafficlaneUpdate
from api.auth.dependencies import require_role
from api.database import db_service
from api.crud import crud_trafficlane

router = APIRouter()


@router.post(
    path="/api/trafficlanes/",
    response_model=TrafficlaneDB | List[TrafficlaneDB] | None,
    dependencies=[Depends(require_role("admin"))])
def create_trafficlane(
    trafficlanes: TrafficlaneCreate | List[TrafficlaneCreate],
    db: Session = Depends(db_service.get_db),
    on_duplicate: str = "raise"):

    try:
        return crud_trafficlane.create(
            db=db,
            obj_in=trafficlanes,
            on_duplicate=on_duplicate)
    except HTTPException as e:
        raise e

@router.get(
        path="/api/trafficlanes",
        response_model=List[TrafficlaneDB])
def read_trafficlanes(
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
    - **List[Trafficlane]**: A list of trafficlane objects.
    """
    try:
        return crud_trafficlane.read_many(
            db=db,
            is_confidential_user=is_confidential_user,
            skip=skip,
            limit=limit)
    except HTTPException as e:
        raise e
