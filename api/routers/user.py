from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette import status

from api.auth.dependencies import get_current_user, require_role
from api.auth.security import get_password_hash
from api.crud import crud_user
from api.database import db_service
from api.models.user import get_user_by_username
from api.schemas import UserPasswordChange, UserCreate, UserDB

router = APIRouter()


@router.post("/api/users", response_model=UserDB, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate,
                      db: Session = Depends(db_service.get_db),
                      _: bool = Depends(require_role("admin"))
                      ):
    """
    Create a new user.

    This endpoint creates a new user with the provided username, password, and role.

    Args:
    - **user** (UserCreate): The user data to be created, including:
        - **username** (str): The username of the user.
        - **password** (str): The password of the user.
        - **role** (str): The role of the user.

    Returns:
    - **UserResponse**: The created user object.

    Raises:
    - **HTTPException**: A 400 error if the username is already registered.
    """
    try:
        db_user = get_user_by_username(user.username, db)
        if db_user:
            raise HTTPException(status_code=400, detail="Username already registered")

        db_user = crud_user.create(db, user)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)

        return UserDB(username=db_user.username, role=db_user.role, id=db_user.id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while creating user: {e}")


@router.put("/api/users/me/change-password")
async def change_password(password_change: UserPasswordChange,
                          db: Session = Depends(db_service.get_db),
                          current_user: UserDB = Depends(get_current_user)):
    """
    Change the password of the current user.

    This endpoint allows the authenticated user to change their password.

    Args:
    - **password_change** (UserPasswordChange): The password change request, including:
        - **new_password** (str): The new password for the user.

    Returns:
    - **dict**: A message indicating the success of the password update.

    Raises:
    - **HTTPException**: A 500 error if an error occurs while updating the password.
    """
    try:
        current_user.hashed_password = get_password_hash(password_change.new_password)
        db.add(current_user)
        db.commit()
        return {"msg": "Password updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred while updating the password: {e}")
