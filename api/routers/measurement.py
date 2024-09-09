from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Union, Annotated

from api.auth.dependencies import require_role
from api.schemas import MeasurementDB, MeasurementCreate, MeasurementUpdate
from api.crud import crud_measurement
from api.database import db_service

router = APIRouter()


@router.post("/api/measurements", response_model=Union[MeasurementDB, list[MeasurementDB], None],
             dependencies=[Depends(require_role("admin"))])
def create_measurement(measurements: Union[MeasurementCreate, list[MeasurementCreate]],
                       db: Session = Depends(db_service.get_db),
                       on_duplicate: str = "raise"):
    """
    Create a new measurements.

    This endpoint accepts either a single measurement object or a list of measurement objects for bulk creation.

    Args:
    - **measurement** (Union[MeasurementCreate, list[MeasurementCreate]]): A sensor object or a list of measurement
      objects. Each measurement object must include:
        - **datastream_id** (int): The ID of the datastream associated with the measurement.
        - **timestamp** (datetime): The timestamp of the measurement.
        - **value** (float): The value of the measurement.
        - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data should
          not be returned to all users.
    - **on_duplicate** (Union[str, None]): Wether to raise error or ignore on duplicate entry

    Returns:
    - **Measurement** (Union[MeasurementCreate, list[MeasurementCreate], None]): The created measurement object or None if no
    measurements were created.
    """
    try:
        return crud_measurement.create(db=db, obj_in=measurements, on_duplicate=on_duplicate, endpoint="measurements")
    except HTTPException as e:
        raise e


@router.get("/api/measurements", response_model=List[MeasurementDB])
def read_measurements(
    skip: Union[int, None] = None,
    limit: Union[int, None] = None,
    datastream_ids: List[int] = Query(default=None),
    order_by: Union[str, None] = None,
    order: str = "ascending",
    db: Session = Depends(db_service.get_db),
    is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a list of measurements with optional pagination.

    Args:
    - **skip** (int, optional): The number of records to skip (for pagination). Defaults to 0.
    - **limit** (int, optional): The maximum number of records to return. Defaults to 100.
    - **datastream_ids** (List[int], optional): List of datastream IDs to query measurements for.
    - **order_by** (str, optional): Attribute to order result by.
    - **order** (str, "ascending" or "descending" default "ascending") Wether to order results asc- or descending.

    Returns:
    - **List[Measurement]**: A list of measurement objects.
    """
    try:
        return crud_measurement.read_many(
            db,
            is_confidential_user,
            skip=skip,
            limit=limit,
            datastream_ids=datastream_ids,
            order_by=order_by,
            order=order)
    except HTTPException as e:
        raise e


@router.get("/api/measurements/{datastream_id}", response_model=List[MeasurementDB])
def read_measurements(
    datastream_id: int,
    limit: Union[int, None] = None,
    from_datetime: Union[str, None] = None,
    to_datetime: Union[str, None] = None,
    db: Session = Depends(db_service.get_db),
    is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve multiple measurements by its datastream ID.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream associated with the measurement.
    - **limit** (int): The max number of entries to retrieve. Defaults to 10.000.
    - **from_datetime** (str): The start date in ISO 8601 format, e. g. "2022-01-01T00:00:00"
    - **to_datetime** (str): The end date  in ISO 8601 format, e. g. "2022-01-01T00:00:00"

    Returns:
    - **List[Measurement]**: The measurement object with the specified datastream ID and timestamp.

    Raises:
    - **HTTPException**: A 404 error if the measurement is not found.
    """
    try:
        if not from_datetime and not to_datetime:
            return crud_measurement.read_many(
                db,
                is_confidential_user,
                limit=limit,
                datastream_id=datastream_id)
        else:
            if from_datetime:
                from_datetime = datetime.fromisoformat(from_datetime)

            if to_datetime:
                to_datetime = datetime.fromisoformat(to_datetime)

            return crud_measurement.get_by_range(
                db=db,
                limit=limit,
                datastream_id=datastream_id,
                from_datetime=from_datetime,
                to_datetime=to_datetime)

    except HTTPException as e:
        raise e



@router.put("/api/measurements/{datastream_id}/{timestamp}", response_model=MeasurementDB)
def update_measurement(datastream_id: int, timestamp: datetime, measurement: MeasurementUpdate,
                       db: Session = Depends(db_service.get_db),
                       _: bool = Depends(require_role("admin"))
                       ):
    """
    Update a measurement by its datastream ID and timestamp.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream associated with the measurement.
    - **timestamp** (datetime): The timestamp of the measurement.
    - **measurement** (MeasurementUpdate): The measurement data to be updated.

    Returns:
    - **Measurement**: The updated measurement object.

    Raises:
    - **HTTPException**: A 404 error if the measurement is not found.
    """
    try:
        db_measurement = crud_measurement.read_single(db, id=datastream_id, timestamp=timestamp)
        return crud_measurement.update(db=db, db_obj=db_measurement, obj_in=measurement)
    except HTTPException as e:
        raise e


@router.delete("/api/measurements/{datastream_id}/{timestamp}", response_model=MeasurementDB)
def delete_measurement(datastream_id: int, timestamp: datetime,
                       db: Session = Depends(db_service.get_db),
                       _: bool = Depends(require_role("admin"))
                       ):
    """
    Delete a measurement by its datastream ID and timestamp.

    Args:
    - **datastream_id** (int): The unique identifier of the datastream associated with the measurement.
    - **timestamp** (datetime): The timestamp of the measurement.

    Returns:
    - **Measurement**: The deleted measurement object.

    Raises:
    - **HTTPException**: A 404 error if the measurement is not found.
    """
    try:
        return crud_measurement.delete(db=db, id=(datastream_id, timestamp))
    except HTTPException as e:
        raise e
