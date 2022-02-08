from dataclasses import dataclass
from typing import Dict, Optional, Union


@dataclass
class Account:
    """
    Account with key and index specific to a transaction.

    Hash and equality only considers the key so accounts across transactions will be equal even with different indices.

    @author zuyezheng
    """

    index: int
    key: str
    signer: Optional[bool] = None
    writable: Optional[bool] = None

    @staticmethod
    def from_value(index: int, value: Union[str, Dict[str, any]]):
        """ Parse from value which depending on extract could be a string or json object. """
        if isinstance(value, str):
            return Account(index, value)
        else:
            return Account(index, value['pubkey'], value['signer'], value['writable'])

    def __hash__(self):
        # accounts in this context are specific to a transaction
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, Account):
            return self.key == other.key

        return NotImplemented
