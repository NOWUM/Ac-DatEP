from .crud import CRUDBase
from api.schemas import TokenCreate, TokenUpdate
from api.models import Token


class CRUDToken(CRUDBase[Token, TokenCreate, TokenUpdate]):
    pass


crud_token = CRUDToken(Token)
