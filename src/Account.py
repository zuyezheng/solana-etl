from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass
class Account:
    """
    Account with key and index specific to a transaction.

    @author zuyezheng
    """
    # first signature of a transaction
    signature: str
    index: int
    key: str

    def __hash__(self):
        # accounts in this context are specific to a transaction
        return hash((self.signature, self.key))

    def __eq__(self, other):
        if isinstance(other, Account):
            return self.signature == other.signature and self.key == other.key

        return NotImplemented


class AccountType(Enum):
    ADDRESS_MAP = 0
    SYSVAR = 1
    PROGRAM = 2
    TOKEN_PROGRAM = 3
    MINT = 4
    TOKEN_ACCOUNT = 5
    ACCOUNT = 6
