from typing import TypeVar, Type, List, Generic, Union

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import inspect, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, Query

from api.models import Base

TCreate = TypeVar("TCreate", bound=BaseModel)
TUpdate = TypeVar("TUpdate", bound=BaseModel)
TModel = TypeVar("TModel", bound=Base)


class CRUDBase(Generic[TModel, TCreate, TUpdate]):
    """
    Generic CRUD object with basic CRUD operations.
    """

    def __init__(self, model: Type[TModel]):
        """
        Initialize the CRUD object with a model.

        Args:
            model (Type[TModel]): A SQLAlchemy model class.
        """
        self.model = model

    def _validate_column_names(self, column_values: dict):
        """
        Validate if the column names provided in column_values exist in the model.
        Raises an HTTPException if any column name is invalid.

        Args:
            column_values (dict): Dictionary representing column names and values.

        Raises:
            HTTPException: If any column name is invalid.
        """
        model_columns = inspect(self.model).columns.keys()
        for column in column_values.keys():
            if column not in model_columns:
                raise HTTPException(status_code=400, detail=f"Invalid column name: {column}")

    def _create_query(
            self,
            db: Session,
            is_confidential_user: bool = False,
            datastream_ids: List[int] = None,
            order_by: Union[str, None] = None,
            order: str = "ascending",
            **column_values: any) -> Query:
        """
        Create a SQLAlchemy query object based on the provided column values.

        Args:
            db (Session): Database session.
            **column_values (any): Keyword arguments representing column names and values.

        Returns:
            query: SQLAlchemy query object.
        """
        self._validate_column_names(column_values)
        query = db.query(self.model)

        if not is_confidential_user:
            query = query.filter(getattr(self.model, 'confidential') == False)

        if datastream_ids:
            query = query.filter(getattr(self.model, 'datastream_id').in_(datastream_ids))

        for column, value in column_values.items():
            query = query.filter(getattr(self.model, column) == value)

        if order_by:
            try:
                if order == "ascending":
                    query = query.order_by((getattr(self.model, order_by)))
                elif order == "descending":
                    query = query.order_by(desc(getattr(self.model, order_by)))
                else:
                    raise HTTPException(400, detail=f"{order} is not a valid order")
            except AttributeError as e:
                raise HTTPException(400, detail=f"{order_by} is not valid value to order by") from e

        return query

    def create(
            self,
            db: Session,
            obj_in: Union[TCreate, list[TCreate]],
            on_duplicate: str = "raise",
            endpoint: str | None = None) -> Union[TModel, list[TModel], None]:
        """
            Create a new record or multiple records.

            Args:
                db (Session): Database session.
                obj_in (Union[TCreate, list[TCreate]]): Data for creating new record(s).

            Returns:
                Union[TModel, list[TModel]]: The created record object(s).
        """
        if not isinstance(obj_in, list):
            obj_in = [obj_in]

        db_objs = []
        for obj in obj_in:
            db_obj = self.model(**obj.dict())
            db_objs.append(db_obj)

        try:
            db.add_all(db_objs)
            db.commit()
        except IntegrityError as e:
            db.rollback()

            if on_duplicate == "raise":
                raise HTTPException(400, detail=f"Integrity Error: {e}")

            elif on_duplicate == "ignore":
                for db_obj in db_objs:
                    try:
                        db.add(db_obj)
                        db.commit()

                    except IntegrityError:
                        db.rollback()
                    except Exception as e:
                        db.rollback()
                        raise HTTPException(500, detail=f"Internal server error: {e}")

        if endpoint == "measurements":
            return None

        if len(db_objs) > 0:
            [db.refresh(obj) for obj in db_objs]

        if len(db_objs) > 1:
            return db_objs
        elif len(db_objs) == 1:
            return db_objs[0]
        elif len(db_objs) == 0:
            print("No db Ojbs")
            return None

    def read_single(self, db: Session, is_confidential_user: bool = False, **column_values: any) -> TModel:
        """
        Get a single record by matching column values.

        Args:
            db (Session): Database session.
            is_confidential_user (bool): Filter data if confidential
            **column_values (any): Keyword arguments representing column names and values.

        Returns:
            TModel: A single record object.
        """
        try:
            query = self._create_query(db, is_confidential_user, **column_values)
            obj = query.first()
            if obj is None:
                raise HTTPException(status_code=404, detail="Object not found")
            return obj
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal Server Error") from e

    def read_many(
            self, db: Session,
            is_confidential_user: bool = False,
            skip: Union[int, None] = 0,
            limit: Union[int, None] = None,
            datastream_ids: Union[List[int], None] = None,
            order_by: Union[str, None] = None,
            order: str = "ascending",
            **column_values: any) -> List[TModel]:
        """
        Get multiple records by matching column values with optional pagination.

        Args:
            db (Session): Database session.
            is_confidential_user (bool): Filter data if confidential
            skip (int, optional): Number of records to skip for pagination. Defaults to 0.
            limit (int, optional): Maximum number of records to return. Defaults to 100.
            **column_values (any): Keyword arguments representing column names and values.

        Returns:
            List[TModel]: List of record objects.
        """
        try:
            query = self._create_query(
                db=db,
                is_confidential_user=is_confidential_user,
                datastream_ids=datastream_ids,
                order_by=order_by,
                order=order,
                **column_values)

            if not limit or limit > 10000:
                limit = 10000

            return query.offset(skip).limit(limit).all()
        except HTTPException as http_e:
            raise http_e
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal Server Error") from e

    def update(self, db: Session, db_obj: TModel, obj_in: TUpdate) -> TModel:
        """
            Update an existing record.

            Args:
                db (Session): Database session.
                db_obj (TModel): The object to be updated.
                obj_in (TUpdate): Data for updating the record.

            Returns:
                TModel: The updated record object.
        """
        try:
            obj_data = obj_in.model_dump()
            for key, value in obj_data.items():
                setattr(db_obj, key, value)
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            return db_obj
        except IntegrityError as e:
            db.rollback()
            raise HTTPException(status_code=400, detail="Integrity Error: {}".format(str(e)))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail="Internal Server Error: {}".format(str(e)))

    def delete(self, db: Session, id: int) -> TModel:
        """
            Delete a record by ID.

            Args:
                db (Session): Database session.
                id (int): Unique identifier of the record to be deleted.

            Returns:
                TModel: The deleted record object.
        """
        obj = self.read_single(db, id=id)
        try:
            db.delete(obj)
            db.commit()
            return obj
        except IntegrityError as e:
            db.rollback()
            raise HTTPException(status_code=500, detail="Integrity Error: {}".format(str(e)))
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail="Failed to delete object: {}".format(str(e)))
