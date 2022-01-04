from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.parse.Instruction import ParsedInstruction
from src.parse.NumberWithScale import NumberWithScale
from src.parse.ProgramInstruction import ProgramInstruction


class Interaction:
    """
    @author zuyezheng
    """

    @staticmethod
    def from_instruction(transaction_signature: str, instruction: ParsedInstruction) -> Interaction:
        if ProgramInstruction.SYSTEM_TRANSFER.of(instruction):
            return Transfer.from_instruction(transaction_signature, instruction)

        raise NotImplementedError


@dataclass
class Transfer(Interaction):
    """
    @author zuyezheng
    """

    transaction_signature: str
    source: str
    destination: str
    value: NumberWithScale
    # mint address of the token or None if base coin
    mint: Optional[str] = None

    @staticmethod
    def from_instruction(transaction_signature: str, instruction: ParsedInstruction) -> Interaction:
        return Transfer.coin(
            transaction_signature,
            instruction.info_accounts['source'].key,
            instruction.info_accounts['destination'].key,
            instruction.info_values['lamports']
        )

    @staticmethod
    def coin(transaction_signature: str, source: str, destination: str, lamports: int) -> Transfer:
        """ Coin interaction with value in lamports. """
        return Transfer(
            transaction_signature=transaction_signature,
            source=source,
            destination=destination,
            value=NumberWithScale(lamports, 9),
            mint=None
        )
