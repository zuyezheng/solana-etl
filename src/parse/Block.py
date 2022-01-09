from __future__ import annotations

import gzip
import json
import time
from functools import cached_property
from pathlib import Path
from typing import Dict

from src.parse.Transaction import Transaction
from src.parse.Transactions import Transactions


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

    def has_transactions(self) -> bool:
        return not self.missing and len(self.result['transactions']) > 0

    def epoch(self) -> int:
        return self.result['blockTime']

    def time(self) -> time:
        return time.gmtime(self.result['blockTime'])

    @cached_property
    def transactions(self) -> Transactions:
        """ Parse and return all transactions in the block. """
        if self.has_transactions():
            return Transactions(list(map(
                lambda t: Transaction(t),
                self.result['transactions']
            )))
        else:
            return Transactions([])

    def find_transaction(self, signature: str) -> Transaction | None:
        """ Linear search for an instruction with the given signature. """
        for transaction in self.transactions:
            if signature in transaction.signatures():
                return transaction

        return None
