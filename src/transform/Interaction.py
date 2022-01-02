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
    def from_instruction(instruction: ParsedInstruction) -> Interaction:
        if ProgramInstruction.SYSTEM_TRANSFER.of(instruction):
            return Transfer.from_instruction(instruction)

        raise NotImplementedError


@dataclass
class Transfer(Interaction):
    """
    @author zuyezheng
    """

    source: str
    destination: str
    value: NumberWithScale
    # mint address of the token or None if base coin
    mint: Optional[str] = None

    @staticmethod
    def from_instruction(instruction: ParsedInstruction) -> Interaction:
        return Transfer.coin(
            instruction.info_accounts['source'].key,
            instruction.info_accounts['destination'].key,
            instruction.info_values['lamports']
        )

    @staticmethod
    def coin(source: str, destination: str, lamports: int) -> Transfer:
        """ Coin interaction with value in lamports. """
        return Transfer(
            source=source,
            destination=destination,
            value=NumberWithScale(lamports, 9),
            mint=None
        )
