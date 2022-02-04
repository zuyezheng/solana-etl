from enum import Enum
from typing import Optional

from src.transform.Instruction import Instructions, Instruction


class ProgramInstruction(Enum):
    """
    Enum of some known instructions.

    @author zuyezheng
    """

    SYSTEM = 'system'
    SYSTEM_TRANSFER = ('system', 'transfer')
    SYSTEM_ALLOCATE = ('system', 'allocate')
    SYSTEM_ASSIGN = ('system', 'assign')
    SYSTEM_CREATE_ACCOUNT = ('system', 'createAccount')

    SPL = 'spl-token'
    SPL_TRANSFER = ('spl-token', 'transfer')

    program_name: str
    instruction_type: Optional[str]

    def __init__(self, program_name: str, instruction_type: Optional[str] = None):
        self.program_name = program_name
        self.instruction_type = instruction_type

    def filter(self, instructions: Instructions, flatten: bool = False) -> Instructions:
        return instructions.filter(self.program_name, self.instruction_type, flatten)

    def of(self, instruction: Instruction) -> bool:
        return instruction.is_of(self.program_name, self.instruction_type)
