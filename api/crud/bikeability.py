from sqlalchemy.orm import Session
import pandas as pd
from fastapi import HTTPException

class CRUDbikeability():
    
    def __init__(self) -> None:
        pass

    def read_many(self, db: Session):

        with db.connection() as conn:
            sql = f"select * from bikeability.bikeability"
            return pd.read_sql(sql, conn).to_dict(orient="records")

crud_bikeability = CRUDbikeability()
