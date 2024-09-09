import os
import jwt

from datetime import datetime, timedelta
from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

PRIVATE_KEY = os.getenv("MOBILITY_API_PRIVATE_KEY")
ALGORITHM = os.getenv("MOBILITY_API_ALGORITHM")
EXPIRE_MINUTES = float(os.getenv("MOBILITY_API_EXPIRE_MINUTES", 30))


def get_password_hash(password):
    """
    Generates a hashed version of a given password.

    This function uses a cryptographic hash function to securely transform a plain text password into a hashed string.
    This hashed string is then used to securely store and verify user passwords without keeping the actual passwords
    in plain text.

    Args:
    - password (str): The plain text password to hash.

    Returns:
    - str: The hashed password.
    """
    return pwd_context.hash(password)


def verify_password(plain_password, hashed_password):
    """
    Verifies a plain text password against a hashed password.

    This function compares a plain text password with a hashed password to verify if they match.
    It is typically used during the authentication process to validate user login attempts.

    Args:
    - plain_password (str): The plain text password to verify.
    - hashed_password (str): The hashed password to compare against.

    Returns:
    - bool: True if the plain text password matches the hashed password, False otherwise.
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    """
    Creates a JWT access token with a specified expiration time.

    This function generates a JWT token containing the provided data along with an expiration time.
    This token is used for authenticating and authorizing users in subsequent requests to the server.

    Args:
    - data (dict): The data to include in the JWT payload, typically user identification information.

    Returns:
    - str: A JWT token encoded with the specified data and an expiration time.
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, PRIVATE_KEY, algorithm=ALGORITHM)


def verify_token(token: str):
    """
    Decodes and verifies a JWT token.

    This function decodes a JWT token to extract its payload and verifies its signature to ensure that the token is
    valid and has not been tampered with. It is used to authenticate requests by verifying the tokens sent by users.

    Args:
    - token (str): The JWT token to decode and verify.

    Returns:
    - dict: The decoded payload of the JWT token if it is valid.

    Raises:
    - jwt.PyJWTError: If the token is invalid or expired, indicating that the authentication is unsuccessful.
    """
    return jwt.decode(token, PRIVATE_KEY, algorithms=[ALGORITHM])
