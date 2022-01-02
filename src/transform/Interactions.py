from typing import List

from parse.Block import Block
from parse.ProgramInstruction import ProgramInstruction
from transform.Interaction import Interaction


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
                    Interaction.from_instruction,
                    ProgramInstruction.SYSTEM_TRANSFER.filter(transaction.instructions, True)
                ))

    def __iter__(self):
        return self.interactions.__iter__()

    def __len__(self):
        return len(self.interactions)
