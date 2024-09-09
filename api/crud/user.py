from sqlalchemy.orm import Session

from .crud import CRUDBase
from ..auth.security import get_password_hash
from api.schemas import UserCreate, UserUpdate
from api.models import User


class CRUDUser(CRUDBase[User, UserCreate, UserUpdate]):
    def create(self, db: Session, obj_in: UserCreate):
        db_obj = User(
            username=obj_in.username,
            hashed_password=get_password_hash(obj_in.password),
            role=obj_in.role
        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj


crud_user = CRUDUser(User)
