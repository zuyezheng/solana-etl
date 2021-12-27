from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property, reduce
from typing import Dict, List, Iterable, Set

from src.Instruction import Instructions, Instruction
from src.NumberWithScale import NumberWithScale


class Transactions:
    """
    Parse a single transaction as part of all Transactions in block.

    @author zuyezheng
    """

    transactions: List[Transaction]

    def __init__(self, transactions: List[Transaction]):
        self.transactions = transactions
        self.size = len(self.transactions)

    def __iter__(self):
        return self.transactions.__iter__()

    def more_than_fee(self) -> List[Transaction]:
        """ Transactions where absolute balance change was greater than the fee. """
        return list(filter(
            lambda t: t.total_account_balance_change() != t.fee(),
            self.transactions
        ))

    def only_fee(self) -> List[Transaction]:
        """ Transactions where only balance change was the fee. """
        return list(filter(
            lambda t: t.total_account_balance_change() == t.fee(),
            self.transactions
        ))


class Transaction:
    meta: Dict[str, any]
    transaction: Dict[str, any]
    # signatures are an array, but they are unique so the first is sufficient as an identifier.
    signature: str
    accounts: List[Account]

    def __init__(self, transaction_meta: Dict[str, any]):
        self.meta = transaction_meta['meta']
        self.transaction = transaction_meta['transaction']
        self.signature = self.transaction['signatures'][0]
        self.accounts = list(map(
            lambda i_key: Account(self.signature, i_key[0], i_key[1]),
            enumerate(self.transaction['message']['accountKeys'])
        ))

    def __hash__(self):
        return hash(self.signature)

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.signature == other.signature

        return NotImplemented

    def is_successful(self):
        return self.meta['err'] is None

    def fee(self):
        return self.meta['fee']

    def pre_balances(self) -> List[int]:
        return self.meta['preBalances']

    def post_balances(self) -> List[int]:
        return self.meta['postBalances']

    def pre_token_balances(self) -> List[Dict[str, any]]:
        return self.meta['preTokenBalances']

    def post_token_balances(self) -> List[Dict[str, any]]:
        return self.meta['postTokenBalances']

    def signatures(self) -> List[str]:
        return self.transaction['signatures']

    @cached_property
    def instructions(self) -> Instructions:
        """ Construct the list of instructions with any nested inner instructions. """
        inner_instructions = {}
        for inner in self.meta['innerInstructions']:
            inner_instructions[inner['index']] = list(map(Instruction.from_json, inner['instructions']))

        instructions = []
        for instruction_i, instruction in enumerate(self.transaction['message']['instructions']):
            instructions.append(Instruction.from_json(
                instruction,
                inner_instructions[instruction_i] if instruction_i in inner_instructions else None
            ))

        return Instructions(instructions)

    def accounts_from_indices(self, indices: Iterable[int]) -> Set[Account]:
        """ Get accounts by their indices in this transaction. """
        return set(map(lambda i: self.accounts[i], indices))

    def programs(self) -> Set[Account]:
        """ Get accounts that are programs. """
        return self.accounts_from_indices(list(self.instructions.program_ids))

    @cached_property
    def account_balance_changes(self) -> Dict[Account, AccountBalanceChange]:
        """ Balance changes by account. """
        changes = {}

        pre_balances = self.pre_balances()
        post_balances = self.post_balances()
        for i, account in enumerate(self.accounts):
            changes[account] = AccountBalanceChange(account, pre_balances[i], post_balances[i])

        return changes

    def total_account_balance_change(self, absolute=True) -> NumberWithScale:
        """ Sum of change of all balances. """
        return reduce(
            lambda a, b: a + b,
            map(
                lambda c: (abs(c.change) if absolute else c.change),
                self.account_balance_changes.values()
            )
        )

    @cached_property
    def token_balance_changes(self) -> Dict[Account, TokenBalanceChange]:
        """ Token changes by account. """
        changes = {}

        pre_balances = self.pre_token_balances()
        post_balances = self.post_token_balances()
        for i, pre in enumerate(pre_balances):
            cur_account = self.accounts[pre['accountIndex']]
            changes[cur_account] = TokenBalanceChange(
                cur_account,
                pre['mint'],
                int(pre['uiTokenAmount']['amount']),
                int(post_balances[i]['uiTokenAmount']['amount']),
                pre['uiTokenAmount']['decimals']
            )

        return changes

    def total_token_changes(self, absolute=True) -> Dict[str, NumberWithScale]:
        """ Sum of token changes by mint address. """
        changes = {}

        for change in self.token_balance_changes.values():
            change_val = abs(change.change) if absolute else change.change

            if change.mint in changes:
                changes[change.mint] += change_val
            else:
                changes[change.mint] = change_val

        return changes


@dataclass
class Account:
    """ Account with key and index specific to a transaction. """
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


class BalanceChange:
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
