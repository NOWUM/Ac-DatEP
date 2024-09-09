import os

import jwt

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.auth.security import verify_token
from api.database import db_service
from api.models.user import get_user_by_username
from api.routers.authentication import oauth2_scheme
from api.schemas import UserDB

PUBLIC_KEY = os.getenv("MOBILITY_API_PUBLIC_KEY")
ALGORITHM = os.getenv("MOBILITY_API_ALGORITHM")


def get_current_user(no_raise: bool = False):
    """
    Creates a wrapper function to retrieve the current user based on the provided JWT token.

    This function generates a wrapper to asynchronously retrieve the current user's details from the database based on
    the authentication token provided in the request headers. It utilizes dependency injection to manage the
    authentication scheme and database session.

    Args:
    - no_raise (bool, optional): If set to True, the function will not raise an exception if the user is not
      authenticated or if the token is invalid. Defaults to False, indicating that exceptions will be raised for
      unauthorized access attempts.

    Returns:
    - A wrapper function (`get_current_user_wrapper`) that takes a JWT token and a database session to authenticate and
      retrieve the user.
    """

    async def get_current_user_wrapper(token: str = Depends(oauth2_scheme), db: Session = Depends(db_service.get_db)):
        return await get_current_user_function(token=token, db=db, no_raise=no_raise)

    return get_current_user_wrapper


async def get_current_user_function(token: str = Depends(oauth2_scheme), db: Session = Depends(db_service.get_db),
                                    no_raise: bool = False):
    """
    Asynchronously retrieves the current user from the database based on the JWT token.

    This function is responsible for decoding the JWT token to extract the username, validating the token, and querying
    the database to retrieve the user's details. If the token is invalid, or the user does not exist in the database, an
    HTTPException is raised unless `no_raise` is set to True.

    Args:
    - token (str): The JWT token used for authentication, typically extracted from the request headers.
    - db (Session): The database session used to query for the user, provided through dependency injection.
    - no_raise (bool, optional): If set to True, the function returns None instead of raising an exception when the user
      is not found or the token is invalid. Defaults to False.

    Returns:
    - The authenticated user object if the token is valid and the user exists in the database. Returns None if
      `no_raise` is True and the user cannot be authenticated.

    Raises:
    - HTTPException: With a 401 Unauthorized status code if the token is invalid, the user does not exist, or any other
      authentication error occurs, unless `no_raise` is True.
    """
    try:
        if not token and no_raise:
            return None

        payload = verify_token(token)
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid authentication credentials") from e
    user = get_user_by_username(username, db)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials")

    return user


def require_role(*required_roles: str, filter_confidential_only: bool = False):
    """
    Decorator to enforce role-based access control on endpoints.

    This decorator is used to enforce that only users with certain roles are allowed to access specific endpoints.
    It can be configured to either completely restrict access to an endpoint or to allow access but filter out confidential
    data for users without the required roles.

    Args:
    - *required_roles (str): Variable length argument list of roles required to access the endpoint without
      restrictions.
    - filter_confidential_only (bool, optional): Flag to indicate whether the endpoint should be accessible to all users
      with confidential data being filtered out for those without the required roles. Defaults to False, meaning that
      access is fully restricted to users without the required roles.

    Returns:
    - A decorator function that checks the user's role before accessing the endpoint.
    """

    def role_checker(current_user: UserDB = Depends(get_current_user(no_raise=filter_confidential_only))) -> bool:
        """
        Inner function to check the current user's role against the required roles.

        This function determines if the current user has one of the required roles to access the endpoint. If
        'filter_confidential_only' is True and the user does not have the required roles, access is still granted but with
        potential filtering of confidential data. Otherwise, access is restricted, and an HTTPException is raised for
        unauthorized users.

        Args:
        - current_user (UserDB): The current user object, typically provided by a dependency injection mechanism like
          FastAPI's Depends.

        Returns:
        - bool: True if the user has access to the endpoint, either by having the required roles or through filtered
          access when 'filter_confidential_only' is True. If the user is unauthorized and 'filter_confidential_only' is False,
          an HTTPException is raised.

        Raises:
        - HTTPException: With a 403 status code if the user does not have the required role(s) and
          'filter_confidential_only' is False.
        """
        if not current_user and filter_confidential_only:
            return False

        if current_user.role in required_roles:
            return True
        raise HTTPException(status_code=403, detail="Operation not permitted")

    return role_checker
