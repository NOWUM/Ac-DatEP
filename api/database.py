import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base
from .auth.security import get_password_hash


from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")


class DatabaseService:
    """
    Service for managing database connections and sessions.
    """

    def __init__(self):
        """
        Initialize the DatabaseService.
        """
        mobility_server = os.getenv("MOBILITY_DB_SERVER", "")
        mobility_port = os.getenv("MOBILITY_DB_PORT", "")
        mobility_database = os.getenv("MOBILITY_DB_NAME", "")
        mobility_username = os.getenv("MOBILITY_DB_USERNAME", "")
        mobility_password = os.getenv("MOBILITY_DB_PASSWORD", "")

        self.engine = create_engine(
            f'postgresql://{mobility_username}:{mobility_password}@{mobility_server}:{mobility_port}/{mobility_database}')
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.db = None

        Base.metadata.create_all(bind=self.engine)

        self.check_create_default_users()

    def check_create_default_users(self):

        api_admin_user = os.getenv("MOBILITY_API_ADMIN_USERNAME")
        api_admin_pw = os.getenv("MOBILITY_API_ADMIN_PASSWORD")
        api_read_all_user = os.getenv("MOBILITY_API_READ_ALL_USER")
        api_read_all_pw = os.getenv("MOBILITY_API_READ_ALL_PASSWORD")


        con = self.engine.raw_connection()
        cur = con.cursor()

        stmt = f"""
            SELECT username
            FROM users
            WHERE username IN ('{api_admin_user}', '{api_read_all_user}')
        """

        cur.execute(stmt)
        res = cur.fetchall()

        if not any([api_admin_user in _ for _ in res]):
            self.create_user(
                con=con,
                username=api_admin_user,
                plain_password=api_admin_pw,
                role='admin')

        if not any([api_read_all_user in _ for _ in res]):
            self.create_user(
                con=con,
                username=api_read_all_user,
                plain_password=api_read_all_pw,
                role='read_all')

        con.commit()
        con.close()

    def create_user(
            self,
            con,
            username,
            plain_password,
            role):

        hashed_pw = get_password_hash(plain_password)

        stmt = f"""
            INSERT INTO users (
                username,
                hashed_password,
                role)
            VALUES (
                '{username}',
                '{hashed_pw}',
                '{role}')
        """

        cur = con.cursor()
        cur.execute(stmt)

    def get_db(self):
        """
        Get a database session.

        Returns:
        - **Session**: A database session.

        Yields:
        - **Session**: A generator that yields a database session.
        """
        if self.db is None:
            self.db = self.SessionLocal()
        try:
            yield self.db
        finally:
            self.db.close()


db_service = DatabaseService()
