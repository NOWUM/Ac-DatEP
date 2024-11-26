from sqlalchemy.orm import Session
import pandas as pd

class CRUDEventDatastream():
    
    def __init__(self) -> None:
        pass

    def read_many(
            self,
            db: Session,
            event: str | None):

        if event:
            where_clause = f"where event = '{event}'"
        else:
            where_clause = ""

        with db.connection() as conn:
            sql = f"select * from event_monitoring.datastreams {where_clause}"
            return pd.read_sql(sql, conn).to_dict(orient="records")

crud_event_datastream = CRUDEventDatastream()
