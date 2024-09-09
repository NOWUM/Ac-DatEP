from datetime import datetime

from typing import Union, List

from sqlalchemy.orm import Session

from .crud import CRUDBase
from api.models import Measurement
from api.schemas import MeasurementCreate, MeasurementUpdate


class CRUDMeasurement(CRUDBase[Measurement, MeasurementCreate, MeasurementUpdate]):

    def get_by_range(
            self,
            db: Session,
            datastream_id: int,
            limit: int | None,
            from_datetime: datetime | None,
            to_datetime: datetime | None):

        if not limit or limit > 10000:
            limit = 10000

        if not from_datetime and not to_datetime:
            return db.query(Measurement).filter(
                Measurement.datastream_id == datastream_id)\
            .limit(limit).all()

        elif not from_datetime and to_datetime:
            return db.query(Measurement).filter(
                Measurement.datastream_id == datastream_id,
                Measurement.timestamp <= to_datetime)\
            .limit(limit).all()

        elif from_datetime and not to_datetime:
            return db.query(Measurement).filter(
                Measurement.datastream_id == datastream_id,
                Measurement.timestamp >= from_datetime)\
            .limit(limit).all()

        else:
            return db.query(Measurement).filter(
                Measurement.datastream_id == datastream_id,
                Measurement.timestamp >= from_datetime,
                Measurement.timestamp <= to_datetime)\
            .limit(limit).all()


crud_measurement = CRUDMeasurement(Measurement)
