from sqlalchemy.orm import Session
import pandas as pd
from fastapi import HTTPException

class CRUDWalkability():
    
    def __init__(self) -> None:
        pass

    def read_many(
            self,
            db: Session,
            persona: str):
        
        if persona not in ["senior", "family", "student"]:
            raise HTTPException(400, "persona does not exist, choose from 'senior', 'family' or 'student'")

        with db.connection() as conn:
            sql = f"select * from walkability.{persona}"
            return pd.read_sql(sql, conn).to_dict(orient="records")

crud_walkability = CRUDWalkability()
