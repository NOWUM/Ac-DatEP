from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Union

from api.auth.dependencies import require_role
from api.schemas import DatastreamDB, DatastreamCreate, DatastreamUpdate
from api.crud import crud_datastream
from api.database import db_service

router = APIRouter()


@router.post("/api/datastreams", response_model=Union[DatastreamDB, list[DatastreamDB], None],
             dependencies=[Depends(require_role("admin"))])
def create_datastream(datastreams: Union[DatastreamCreate, list[DatastreamCreate]],
                      db: Session = Depends(db_service.get_db),
                      on_duplicate: str = "raise"):
    """
    Create a new datastreams.

    This endpoint accepts either a single datastream object or a list of datastream objects for bulk creation.

    Args:
    - **datastreams** (DatastreamCreate): The datastream object to be created. It must include:
        - **sensor_id** (int): The ID of the sensor associated with the datastream.
        - **ex_id** (int): An external identifier for the datastream.
        - **type** (str): The type of data stored in the datastream (e.g., temperature, air pressure, parking sensor,
          traffic congestion, etc.).
        - **unit** (str): The unit of measurement for the data.
        - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data
          should not be returned to all users.

    Returns:
    - **Datastream**: The created datastream object.
    """
    try:
        return crud_datastream.create(db=db, obj_in=datastreams, on_duplicate=on_duplicate)
    except HTTPException as e:
        raise e


@router.get("/api/datastreams", response_model=List[DatastreamDB])
def read_datastreams(skip: Union[int, None] = None, limit: Union[int, None] = None,
                     db: Session = Depends(db_service.get_db),
                     is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a list of datastreams with optional pagination.

    Args:
    - **skip** (int, optional): The number of records to skip (for pagination). Defaults to 0.
    - **limit** (int, optional): The maximum number of records to return. Defaults to 100.

    Returns:
    - **List[Datastream]**: A list of datastream objects.
    """
    try:
        return crud_datastream.read_many(db, is_confidential_user, skip=skip, limit=limit)
    except HTTPException as e:
        raise e


@router.get("/api/datastreams/{datastream_id}", response_model=DatastreamDB)
def read_datastream(datastream_id: int,
                    db: Session = Depends(db_service.get_db),
                    is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a single datastream by its ID.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream.

    Returns:
    - **Datastream**: The datastream object with the specified ID.

    Raises:
    - **HTTPException**: A 404 error if the datastream is not found.
    """
    try:
        return crud_datastream.read_single(db, is_confidential_user, id=datastream_id)
    except HTTPException as e:
        raise e


@router.put("/api/datastreams/{datastream_id}", response_model=DatastreamDB)
def update_datastream(datastream_id: int, datastream: DatastreamUpdate,
                      db: Session = Depends(db_service.get_db),
                      _: bool = Depends(require_role("admin"))
                      ):
    """
    Update a datastream by its ID.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream to be updated.
    - **datastream** (DatastreamUpdate): The datastream data to be updated.

    Returns:
    - **Datastream**: The updated datastream object.

    Raises:
    - **HTTPException**: A 404 error if the datastream is not found.
    """
    try:
        db_datastream = crud_datastream.read_single(db, id=datastream_id)
        return crud_datastream.update(db=db, db_obj=db_datastream, obj_in=datastream)
    except HTTPException as e:
        raise e


@router.delete("/api/datastreams/{datastream_id}", response_model=DatastreamDB)
def delete_datastream(datastream_id: int,
                      db: Session = Depends(db_service.get_db),
                      _: bool = Depends(require_role("admin"))
                      ):
    """
    Delete a datastream by its ID.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream to be deleted.

    Returns:
    - **Datastream**: The deleted datastream object.

    Raises:
    - **HTTPException**: A 404 error if the datastream is not found.
    """
    try:
        return crud_datastream.delete(db=db, id=datastream_id)
    except HTTPException as e:
        raise e
