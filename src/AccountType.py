from enum import Enum

from src.Account import Account
from src.Transaction import Transaction


class AccountType(Enum):
    ADDRESS_MAP = 0
    SYSVAR = 1
    PROGRAM = 2
    TOKEN_PROGRAM = 3
    MINT = 4
    TOKEN_ACCOUNT = 5
    ACCOUNT = 6

    @staticmethod
    # move to
    def from_account(account: Account, transaction: Transaction):
        if account.key == '11111111111111111111111111111111':
            return AccountType.ADDRESS_MAP
        elif account.key.startswith('Sysvar'):
            return AccountType.SYSVAR
        else:
            return AccountType.ACCOUNT

        pass
