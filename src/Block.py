from __future__ import annotations

import gzip
import json
import time
from functools import reduce
from pathlib import Path
from typing import Dict, List, Set


class Block:
    """
    Parse JSON metadata for a block.

    @author zuyezheng
    """

    result: Dict[str, any] | None
    missing: bool

    @staticmethod
    def open(path: Path):
        def _open():
            if path.suffix == '.gz':
                return gzip.open(path)
            else:
                return open(path)

        with _open() as f:
            return Block(json.load(f))

    def __init__(self, block_meta: Dict):
        if 'result' in block_meta:
            self.result = block_meta['result']
            self.missing = False
        else:
            self.result = None
            self.missing = True

        self._transactions = None

    def has_transactions(self):
        return not self.missing and len(self.result['transactions'] > 0)

    def block_time(self):
        return time.gmtime(self.result['blockTime'])

    def transactions(self):
        if self._transactions is None:
            self._transactions = list(map(
                lambda t: Transaction(t),
                self.result['transactions']
            ))

        return self._transactions

    def find_transaction(self, signature: str) -> Transaction | None:
        """ Linear search for an instruction with the given signature. """
        for transaction in self.transactions():
            if signature in transaction.signatures():
                return transaction

        return None


class Instruction:
    accounts: List[int]
    data: str
    program_id_index: int
    inner_instructions: Instructions

    program_ids: Set[int]

    def __init__(
        self,
        accounts: List[int],
        data: str,
        program_id_index: int,
        inner_instructions: List[Instruction] = None
    ):
        self.accounts = accounts
        self.data = data
        self.program_id_index = program_id_index
        self.inner_instructions = Instructions([] if inner_instructions is None else inner_instructions)

        self.program_ids = { self.program_id_index } | self.inner_instructions.program_ids

    def count(self) -> int:
        return self.inner_instructions.count + 1

    @staticmethod
    def from_json(json_data: Dict[str, any], inner_instructions: List[Instruction] = None) -> Instruction:
        return Instruction(
            accounts=json_data['accounts'],
            data=json_data['data'],
            program_id_index=json_data['programIdIndex'],
            inner_instructions=inner_instructions
        )


class Instructions:
    instructions: List[Instruction]
    count: int
    program_ids: Set[int]

    def __init__(self, instructions: List[Instruction]):
        self.instructions = instructions

        self.count = sum(map(lambda i: i.count(), self.instructions))
        self.program_ids = reduce(lambda a, b: a | b, map(lambda i: i.program_ids, self.instructions), set())

    def __iter__(self):
        return self.instructions.__iter__()


class Transaction:

    def __init__(self, transaction_meta: Dict):
        self.meta = transaction_meta['meta']
        self.transaction = transaction_meta['transaction']

        self._instructions = None

    def is_successful(self):
        return self.meta['err'] is None

    def fee(self):
        return self.meta['fee']

    def balance_changes(self) -> Dict[str, int]:
        """ Balance changes by account key. """
        changes = {}

        pre_balances = self.pre_balances()
        post_balances = self.post_balances()
        for i, account_key in enumerate(self.account_keys()):
            changes[account_key] = post_balances[i] - pre_balances[i]

        return changes

    def total_balance_change(self, absolute=True) -> int:
        """ Sum of change of all balances. """
        total = 0

        for account_key, change in self.balance_changes().items():
            if absolute:
                total += abs(change)
            else:
                total += change

        return total

    def pre_balances(self) -> List[int]:
        return self.meta['preBalances']

    def post_balances(self) -> List[int]:
        return self.meta['postBalances']

    def signature(self) -> str:
        """
        Signatures is technically an array, but likely there is only one which is the one you care about and most will
        index by the first.
        """
        return self.signatures()[0]

    def signatures(self) -> List[str]:
        return self.transaction['signatures']

    def account_keys(self) -> List[str]:
        return self.transaction['message']['accountKeys']

    def instructions(self) -> Instructions:
        """
        Construct the list of instructions with any nested inner instructions.
        """
        if self._instructions is None:
            inner_instructions = {}
            for inner in self.meta['innerInstructions']:
                inner_instructions[inner['index']] = list(map(Instruction.from_json, inner['instructions']))

            instructions = []
            for instruction_i, instruction in enumerate(self.transaction['message']['instructions']):
                instructions.append(Instruction.from_json(
                    instruction,
                    inner_instructions[instruction_i] if instruction_i in inner_instructions else None
                ))

            self._instructions = Instructions(instructions)

        return self._instructions
