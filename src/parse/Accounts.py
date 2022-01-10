from typing import List, Dict, Iterable, Set, Optional, Union

from src.parse.Account import Account


class Accounts:
    """
    Collection of accounts in a transaction with possible typing.

    @author zuyezheng
    """

    transaction_signature: str

    _accounts: List[Account]
    _accounts_by_key: Dict[str, Account]

    @staticmethod
    def from_json(transaction_signature: str, account_keys: List[Union[str, Dict[str, any]]]):
        return Accounts(
            transaction_signature,
            list(map(
                lambda i_key: Account.from_value(i_key[0], i_key[1]),
                enumerate(account_keys)
            ))
        )

    def __init__(self, transaction_signature: str, accounts: List[Account]):
        self.transaction_signature = transaction_signature

        self._accounts = accounts
        self._accounts_by_key = dict(map(
            lambda account: (account.key, account),
            self._accounts
        ))

    def __iter__(self):
        return self._accounts.__iter__()

    def __getitem__(self, key: str) -> Account:
        """ Get account by public key. """
        return self._accounts_by_key[key]

    def __len__(self):
        return len(self._accounts)

    def keys(self) -> Iterable[str]:
        return self._accounts_by_key.keys()

    def get(self, key: any) -> Optional[Account]:
        return self._accounts_by_key.get(key)

    def get_index(self, index: int) -> Account:
        """ Get account by index it appears in the transaction. """
        return self._accounts[index]

    def from_indices(self, indices: Iterable[int]) -> Set[Account]:
        """ Get accounts by their indices in this transaction. """
        return {self._accounts[i] for i in indices}

    def from_keys(self, keys: Iterable[str]) -> Set[Account]:
        """ Return accounts for the given keys. """
        return {self._accounts_by_key[k] for k in keys}
