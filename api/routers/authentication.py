from fastapi import APIRouter, Depends, status, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from api.auth.security import verify_password, create_access_token
from api.database import db_service
from api.models.user import get_user_by_username

router = APIRouter()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token", auto_error=False)


@router.post("/api/auth/token", include_in_schema=False)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(),
                                 db: Session = Depends(db_service.get_db)):
    """
    Retrieve an access token for authentication.

    This endpoint authenticates a user and returns an access token.

    Args:
    - **form_data** (OAuth2PasswordRequestForm): The login form data containing username and password.

    Returns:
    - **dict**: An access token and token type.

    Raises:
    - **HTTPException**: A 401 error if the username or password is incorrect.
    """
    user = get_user_by_username(form_data.username, db)

    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}
