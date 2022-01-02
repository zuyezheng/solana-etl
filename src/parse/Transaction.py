from __future__ import annotations

from functools import cached_property, reduce
from typing import Dict, List, Set

from src.parse.Account import Account
from src.parse.Accounts import Accounts
from src.parse.BalanceChange import TokenBalanceChange, AccountBalanceChange, BalanceChangeAgg
from src.parse.Instruction import Instructions, Instruction
from src.parse.NumberWithScale import NumberWithScale


class Transaction:
    """
    Parse out a transaction and put together some interesting pieces of metadata.

    @author zuyezheng
    """

    meta: Dict[str, any]
    transaction: Dict[str, any]
    # signatures are an array, but they are unique so the first is sufficient as an identifier.
    signature: str
    accounts: Accounts

    def __init__(self, transaction_meta: Dict[str, any]):
        self.meta = transaction_meta['meta']
        self.transaction = transaction_meta['transaction']
        self.signature = self.transaction['signatures'][0]
        self.accounts = Accounts.from_json(self.signature, self.transaction['message']['accountKeys'])

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
            inner_instructions[inner['index']] = list(map(
                lambda data: Instruction.factory(self.accounts, data),
                inner['instructions']
            ))

        instructions = []
        for instruction_i, instruction in enumerate(self.transaction['message']['instructions']):
            instructions.append(Instruction.factory(
                self.accounts, instruction, inner_instructions.get(instruction_i)
            ))

        return Instructions(instructions).set_ids()

    @cached_property
    def account_balance_changes(self) -> Dict[Account, AccountBalanceChange]:
        """ Balance changes by account. """
        changes = {}

        pre_balances = self.pre_balances()
        post_balances = self.post_balances()
        for i, account in enumerate(self.accounts):
            changes[account] = AccountBalanceChange(account, pre_balances[i], post_balances[i])

        return changes

    def total_account_balance_change(self, agg: BalanceChangeAgg = BalanceChangeAgg.ALL) -> NumberWithScale:
        """ Sum of change of all balances. """
        return reduce(
            lambda a, b: a + b,
            map(
                lambda c: agg(c.change),
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
            cur_account = self.accounts.get_index(pre['accountIndex'])
            changes[cur_account] = TokenBalanceChange(
                cur_account,
                pre['mint'],
                int(pre['uiTokenAmount']['amount']),
                int(post_balances[i]['uiTokenAmount']['amount']),
                pre['uiTokenAmount']['decimals']
            )

        return changes

    def total_token_changes(self, agg: BalanceChangeAgg = BalanceChangeAgg.ALL) -> Dict[str, NumberWithScale]:
        """ Sum of token changes by mint address. """
        changes = {}

        for change in self.token_balance_changes.values():
            if change.mint in changes:
                changes[change.mint] += agg(change.change)
            else:
                changes[change.mint] = agg(change.change)

        return changes

    @property
    def mints(self) -> Set[str]:
        """ All token mints in the transaction. """
        return {change.mint for change in self.token_balance_changes.values()}
