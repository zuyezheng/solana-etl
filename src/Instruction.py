from __future__ import annotations

from functools import reduce
from typing import Dict, List, Set


class Instructions:
    """
    Parse out outer and inner instructions for a transaction.

    @author zuyezheng
    """

    instructions: List[Instruction]
    size: int
    program_ids: Set[int]

    def __init__(self, instructions: List[Instruction]):
        self.instructions = instructions

        self.size = sum(map(lambda i: i.size, self.instructions))
        self.program_ids = reduce(lambda a, b: a | b, map(lambda i: i.program_ids, self.instructions), set())

    def __iter__(self):
        return self.instructions.__iter__()


class Instruction:
    accounts: List[int]
    data: str
    program_id_index: int
    inner_instructions: Instructions

    program_ids: Set[int]

    @staticmethod
    def from_json(json_data: Dict[str, any], inner_instructions: List[Instruction] = None) -> Instruction:
        return Instruction(
            accounts=json_data['accounts'],
            data=json_data['data'],
            program_id_index=json_data['programIdIndex'],
            inner_instructions=inner_instructions
        )

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
        self.size = self.inner_instructions.size + 1

