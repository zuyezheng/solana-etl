from functools import partial
from typing import List

from src.parse.Block import Block
from src.parse.ProgramInstruction import ProgramInstruction
from src.transform.Interaction import Interaction


class Interactions:
    """
    Form interactions from a list of blocks.

    @author zuye.zheng
    """

    interactions: List[Interaction]

    def __init__(self, blocks: List[Block]):
        self.interactions = []
        for block in blocks:
            for transaction in block.transactions:
                self.interactions.extend(map(
                    partial(Interaction.from_instruction, transaction.signature),
                    ProgramInstruction.SYSTEM_TRANSFER.filter(transaction.instructions, True)
                ))

    def __iter__(self):
        return self.interactions.__iter__()

    def __len__(self):
        return len(self.interactions)
