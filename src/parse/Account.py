from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Union


@dataclass
class Account:
    """
    Account with key and index specific to a transaction.

    This is intended to be used with Accounts so hash and eq are only valid for within a transaction where index of an
    account is consistent.

    @author zuyezheng
    """

    index: int
    key: str
    signer: Optional[bool] = None
    writable: Optional[bool] = None

    @staticmethod
    def from_value(index: int, value: Union[str, Dict[str, any]]):
        """ Parse from value which depending on extract could be a string or json object. """
        if isinstance(value, str):
            return Account(index, value)
        else:
            return Account(index, value['pubkey'], value['signer'], value['writable'])

    def __hash__(self):
        # accounts in this context are specific to a transaction
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, Account):
            return self.key == other.key

        return NotImplemented


class AccountType(Enum):
    ADDRESS_MAP = 0
    SYSVAR = 1
    PROGRAM = 2
    TOKEN_PROGRAM = 3
    MINT = 4
    TOKEN_ACCOUNT = 5
    ACCOUNT = 6
