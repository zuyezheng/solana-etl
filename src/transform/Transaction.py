from __future__ import annotations

from functools import cached_property, reduce
from typing import Dict, List, Set

from src.transform.Account import Account
from src.transform.AccountType import AccountType
from src.transform.Accounts import Accounts
from src.transform.BalanceChange import TokenBalanceChange, AccountBalanceChange, BalanceChangeAgg
from src.transform.Instruction import Instructions, Instruction
from src.transform.NumberWithScale import NumberWithScale


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

    @property
    def is_successful(self) -> bool:
        return self.meta['err'] is None

    @property
    def fee(self) -> int:
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

        # for token balances, if a new account is created it will not be included in pre, if an account is closed it
        # will not be in post so map it by account index
        def balances_by_index(balances: List[Dict[str, any]]) -> Dict[int, Dict[str, any]]:
            return dict(map(
                lambda b: (b['accountIndex'], b),
                balances
            ))

        pre_balances = balances_by_index(self.pre_token_balances())
        post_balances = balances_by_index(self.post_token_balances())

        # set of all account indices across pre and post
        account_indices = pre_balances.keys() | post_balances.keys()

        changes = {}
        for index in account_indices:
            cur_account = self.accounts.get_index(index)

            # start or end value are 0 if missing pre or post
            start = 0
            end = 0

            if index in pre_balances:
                balance = pre_balances[index]
                start = int(balance['uiTokenAmount']['amount'])

            if index in post_balances:
                balance = post_balances[index]
                end = int(balance['uiTokenAmount']['amount'])

            changes[cur_account] = TokenBalanceChange(
                cur_account, balance['mint'], start, end, balance['uiTokenAmount']['decimals']
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

    @cached_property
    def accounts_by_type(self) -> Dict[AccountType, Set[Account]]:
        program_accounts = self.instructions.programs
        token_accounts = set(self.token_balance_changes.keys())

        sysvar_accounts = set()
        coin_accounts = set()
        for account in self.accounts:
            if account.key.lower().startswith('sysvar'):
                sysvar_accounts.add(account)
            elif account not in program_accounts and account not in token_accounts:
                coin_accounts.add(account)

        return {
            AccountType.SYSVAR: sysvar_accounts,
            AccountType.PROGRAM: program_accounts,
            AccountType.TOKEN: token_accounts,
            AccountType.COIN: coin_accounts
        }
