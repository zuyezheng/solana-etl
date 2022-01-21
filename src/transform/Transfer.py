from dataclasses import dataclass

from src.parse.Instruction import ParsedInstruction
from src.parse.NumberWithScale import NumberWithScale
from src.parse.Transaction import Transaction
from src.transform.Interaction import Interaction


@dataclass
class Transfer(Interaction):
    """
    @author zuyezheng
    """

    transaction_signature: str
    source: str
    destination: str
    value: NumberWithScale


class CoinTransfer(Transfer):

    @staticmethod
    def from_instruction(transaction: Transaction, instruction: ParsedInstruction) -> Transfer:
        """ Coin transfer with value in lamports. """
        return CoinTransfer(
            transaction_signature=transaction.signature,
            source=instruction.info_accounts['source'].key,
            destination=instruction.info_accounts['destination'].key,
            value=NumberWithScale.lamports(instruction.info_values['lamports'])
        )


@dataclass
class TokenTransfer(Transfer):
    authority: str
    multisig: bool
    mint: str

    @staticmethod
    def from_instruction(transaction: Transaction, instruction: ParsedInstruction) -> Transfer:
        """ Token transfer. """
        source = instruction.info_accounts['source']
        destination = instruction.info_accounts['destination']

        # need some info about the mint and token scale which is in balance change, using either source or destination
        balance_changes = transaction.token_balance_changes

        if source in balance_changes:
            balance_change = balance_changes[source]
        elif destination in balance_changes:
            balance_change = balance_changes[destination]
        else:
            raise Exception()

        if 'authority' in instruction.info_accounts:
            authority = instruction.info_accounts['authority'].key
            multisig = False
        else:
            authority = instruction.info_accounts['multisigAuthority'].key
            multisig = True

        return TokenTransfer(
            transaction_signature=transaction.signature,
            source=source.key,
            destination=destination.key,
            value=NumberWithScale(int(instruction.info_values['amount']), balance_change.start.scale),
            authority=authority,
            multisig=multisig,
            mint=balance_change.mint
        )
