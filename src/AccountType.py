from enum import Enum, auto

from src.Account import Account
from src.Transaction import Transaction


class AccountType(Enum):
    SYSVAR = auto()
    PROGRAM = auto()
    TOKEN_PROGRAM = auto()
    TOKEN_ACCOUNT = auto()
    ACCOUNT = auto()

    @staticmethod
    # move to
    def from_account(account: Account, transaction: Transaction):
        if account.key.startswith('Sysvar'):
            return AccountType.SYSVAR
        else:
            return AccountType.ACCOUNT

        pass
