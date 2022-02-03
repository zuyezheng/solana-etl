from enum import Enum

from src.transform.Account import Account
from src.transform.NumberWithScale import NumberWithScale


class BalanceChange:
    """
    Encapsulate various balance change types.

    @author zuyehzheng
    """

    account: Account
    start: NumberWithScale
    end: NumberWithScale
    change: NumberWithScale

    def __init__(self, account: Account, start: int, end: int, decimals: int):
        self.account = account
        self.start = NumberWithScale(start, decimals)
        self.end = NumberWithScale(end, decimals)
        self.change = NumberWithScale(end - start, decimals)


class AccountBalanceChange(BalanceChange):

    def __init__(self, account: Account, start: int, end: int):
        super().__init__(account, start, end, 9)


class TokenBalanceChange(BalanceChange):

    mint: str

    def __init__(self, account: Account, mint: str, start: int, end: int, decimals: int):
        super().__init__(account, start, end, decimals)

        self.mint = mint


class BalanceChangeAgg(Enum):

    # aggregate all changes
    ALL = 0
    # all changes, but as absolutes
    ABS = 1
    # only use incoming (positive) changes
    IN = 2
    # only use outgoing (negative) changes
    OUT = 3

    def __call__(self, v: NumberWithScale):
        if self == BalanceChangeAgg.ALL:
            return v
        elif self == BalanceChangeAgg.ABS:
            return abs(v)
        elif self == BalanceChangeAgg.IN:
            return v if v.v > 0 else v.zero()
        elif self == BalanceChangeAgg.OUT:
            return v if v.v < 0 else v.zero()

        return NotImplemented
