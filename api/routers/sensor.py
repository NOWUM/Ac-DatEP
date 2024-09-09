from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Union

from api.auth.dependencies import require_role
from api.schemas import SensorDB, SensorCreate, SensorUpdate
from api.crud import crud_sensor
from api.database import db_service

router = APIRouter()


@router.post("/api/sensors", response_model=Union[SensorDB, list[SensorDB], None],
             dependencies=[Depends(require_role("admin"))])
def create_sensor(sensors: Union[SensorCreate, list[SensorCreate]],
                  db: Session = Depends(db_service.get_db),
                  on_duplicate: str = "raise"):
    """
    Create a new sensor or a list of sensors.

    This endpoint accepts either a single sensor object or a list of sensor objects for bulk creation.

    Args:
    - **sensors** (Union[SensorCreate, list[SensorCreate]]): A sensor object or a list of sensor objects. Each sensor
      object must include:
        - **source** (str): The source of the sensor data.
        - **ex_id** (Optional[int]): An external identifier for the sensor.
        - **description** (Optional[str]): A description of the sensor.
        - **longitude** (float): The longitude coordinate of the sensor.
        - **latitude** (float): The latitude coordinate of the sensor.
        - **confidential** (bool): A flag indicating whether the sensor data is confidential. If set to True, the data should
          not be returned to all users.

    Returns:
    - **Sensor(s)**: The created sensor object(s) with their assigned database ID.
    """
    try:
        return crud_sensor.create(db=db, obj_in=sensors, on_duplicate=on_duplicate)
    except HTTPException as e:
        raise e


@router.get("/api/sensors", response_model=List[SensorDB])
def read_sensors(skip: Union[int, None] = None, limit: Union[int, None] = None,
                 db: Session = Depends(db_service.get_db),
                 is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a list of sensors with optional pagination.

    Args:
    - **skip** (int, optional): The number of records to skip (for pagination). Defaults to 0.
    - **limit** (int, optional): The maximum number of records to return. Defaults to 100.

    Returns:
    - **Sensors**: A list of sensor objects.
    """
    try:
        return crud_sensor.read_many(db, is_confidential_user, skip=skip, limit=limit)
    except HTTPException as e:
        raise e


@router.get("/api/sensors/{sensor_id}", response_model=SensorDB)
def read_sensor(sensor_id: int,
                db: Session = Depends(db_service.get_db),
                is_confidential_user: bool = Depends(require_role('admin', 'read_all', filter_confidential_only=True))):
    """
    Retrieve a single sensor by its ID.

    Args:
    - **sensor_id** (int): The unique identifier of the sensor.

    Returns:
    - **Sensor**: The sensor object with the specified ID.

    Raises:
    - **HTTPException**: A 404 error if the sensor is not found.
    """
    try:
        return crud_sensor.read_single(db, is_confidential_user, id=sensor_id)
    except HTTPException as e:
        raise e


@router.put("/api/sensors/{sensor_id}", response_model=SensorDB)
def update_sensor(sensor_id: int, sensor: SensorUpdate,
                  db: Session = Depends(db_service.get_db),
                  _: bool = Depends(require_role("admin"))
                  ):
    """
    Update a sensor by its ID.

    Args:
    - **sensor_id** (int): The unique identifier of the sensor to be updated.
    - **sensor** (SensorUpdate): The sensor data to be updated. Can include:
        - **source** (str): The source of the sensor data.
        - **ex_id** (Optional[int]): An external identifier for the sensor.
        - **description** (Optional[str]): A description of the sensor.
        - **longitude** (float): The longitude coordinate of the sensor.
        - **latitude** (float): The latitude coordinate of the sensor.

    Returns:
    - **Sensor**: The updated sensor object.

    Raises:
    - **HTTPException**: A 404 error if the sensor is not found.
    """
    try:
        db_sensor = crud_sensor.read_single(db, id=sensor_id)
        return crud_sensor.update(db=db, db_obj=db_sensor, obj_in=sensor)
    except HTTPException as e:
        raise e


@router.delete("/api/sensors/{sensor_id}", response_model=SensorDB)
def delete_sensor(sensor_id: int,
                  db: Session = Depends(db_service.get_db),
                  _: bool = Depends(require_role("admin"))
                  ):
    """
    Delete a sensor by its ID.

    Args:
    - **sensor_id** (int): The unique identifier of the sensor to be deleted.

    Returns:
    - **Sensor**: The deleted sensor object.

    Raises:
    - **HTTPException**: A 404 error if the sensor is not found.
    """
    try:
        return crud_sensor.delete(db=db, id=sensor_id)
    except HTTPException as e:
        raise e
