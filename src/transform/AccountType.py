from enum import Enum, auto


class AccountType(Enum):
    """
    @author zuyezheng
    """

    SYSVAR = auto()
    PROGRAM = auto()
    # token based account
    TOKEN = auto()
    # coin based account
    COIN = auto()
