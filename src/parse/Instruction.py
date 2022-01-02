from __future__ import annotations

from abc import abstractmethod
from functools import reduce, cached_property
from typing import Dict, List, Set, Optional

from src.parse.Account import Account
from src.parse.Accounts import Accounts


class Instruction:
    """
    Make sense of partially and fully parsed instructions and put together inner with outer instructions from different
    parts of the transaction.

    @author zuyezheng
    """

    program_key: str
    inner_instructions: Instructions

    # all accounts used in this and inner instructions
    accounts: Set[Account]
    # all program accounts in this and any inner instructions
    program: Account

    # recursively generated id that uses the order this appears in Instructions
    gen_id: Optional[str]

    @staticmethod
    def factory(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: List[Instruction] = None
    ) -> Instruction:
        inner_instructions = Instructions([] if inner_instructions is None else inner_instructions)
        if 'parsed' in json_data:
            return ParsedInstruction.from_json(all_accounts, json_data, inner_instructions)
        else:
            return PartiallyParsedInstruction.from_json(all_accounts, json_data, inner_instructions)

    def __init__(
        self,
        # accounts used in this instruction
        accounts: Set[Account],
        # program key
        program: Account,
        inner_instructions: Instructions,
        # generated string, probably should leave this to Instructions.set_ids unless copy
        gen_id: str = None
    ):
        self.inner_instructions = inner_instructions
        self.accounts = accounts
        self.program = program
        self.gen_id = gen_id

    def __len__(self):
        return len(self.inner_instructions) + 1

    @cached_property
    def programs(self) -> Set[Account]:
        """ All programs in the current and any inner instructions. """
        return {self.program} | self.inner_instructions.programs

    def set_id(self, parent: Optional[str], index: int) -> Instruction:
        """ See Instructions.set_ids. """
        self.gen_id = str(index) if parent is None else f'{parent}.{index}'
        self.inner_instructions.set_ids(self.gen_id)

        return self

    def flatten(self) -> Instructions:
        """
        Flatten this and any inner into a single collection, new outer instructions will be created without empty inner.
        """
        return Instructions([self.copy()] + self.inner_instructions.flatten().instructions)

    def filter(self, program_name: str, instruction_type: Optional[str]) -> Optional[Instruction]:
        """
        Return a copy of this Instruction with inner instructions filtered by the same parameters. If this and no inner
        instructions match the filter, return None.
        """
        inner_filtered = self.inner_instructions.filter(program_name, instruction_type)

        if inner_filtered or self.is_of(program_name, instruction_type):
            return self.copy(inner_filtered)
        else:
            return None

    @abstractmethod
    def is_of(self, program_name: str, instruction_type: Optional[str]) -> bool:
        """ If the current instruction is of the program name and optional instruction type. """
        raise NotImplementedError

    @abstractmethod
    def copy(self, inner: Optional[Instructions] = None) -> Instruction:
        """ Create a copy of this instruction with new inner instructions. """
        raise NotImplementedError


class PartiallyParsedInstruction(Instruction):
    """ Instruction where we only know about the accounts and program involved and not the actual operations. """

    data: str

    @staticmethod
    def from_json(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: Optional[Instructions]
    ) -> PartiallyParsedInstruction:
        return PartiallyParsedInstruction(
            all_accounts.from_keys(json_data['accounts']),
            all_accounts[json_data['programId']],
            inner_instructions,
            json_data['data']
        )

    def __init__(
        self,
        accounts: Set[Account],
        program: Account,
        inner_instructions: Optional[Instructions],
        data: str,
        gen_id: Optional[str] = None
    ):
        super().__init__(
            accounts, program, Instructions([] if inner_instructions is None else inner_instructions), gen_id
        )

        self.data = data

    def is_of(self, program_name: str, instruction_type: Optional[str]) -> bool:
        # not checkable since only partially parsed
        return False

    def copy(self, inner: Optional[Instructions] = None) -> Instruction:
        return PartiallyParsedInstruction(
            self.accounts, self.program, inner, self.data, self.gen_id
        )


class ParsedInstruction(Instruction):

    program_name: str
    instruction_type: Optional[str]
    info_accounts: Dict[str, Account]
    info_values: Dict[str, any]

    @staticmethod
    def from_json(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: Optional[Instructions]
    ) -> ParsedInstruction:
        # need to do some work to figure out which of the arguments are account keys
        info_accounts = {}
        info_values = {}

        if isinstance(json_data['parsed'], dict):
            # most parsed instructions will be a json dict with value and accounts
            instruction_type = json_data['parsed']['type']
            for info_key, info_value in json_data['parsed']['info'].items():
                # see if the value is an account key or another instruction argument, probably should look deeper into
                # nested object arguments, but seems to cover all current use cases
                account = all_accounts.get(info_value) if isinstance(info_value, str) else None

                if account is None:
                    info_values[info_key] = info_value
                else:
                    info_accounts[info_key] = account
        else:
            # programs like spl-memo will just have an encoded value in parsed
            instruction_type = None
            info_values[None] = json_data['parsed']

        return ParsedInstruction(
            all_accounts[json_data['programId']],
            inner_instructions,
            json_data['program'],
            instruction_type,
            info_accounts,
            info_values
        )

    def __init__(
        self,
        program: Account,
        inner_instructions: Optional[Instructions],
        program_name: str,
        instruction_type: Optional[str],
        info_accounts: Dict[str, Account],
        info_values: Dict[str, any],
        gen_id: Optional[str] = None
    ):
        super().__init__(
            set(info_accounts.values()),
            program,
            Instructions([] if inner_instructions is None else inner_instructions),
            gen_id
        )

        self.program_name = program_name
        self.instruction_type = instruction_type
        self.info_accounts = info_accounts
        self.info_values = info_values

    def is_of(self, program_name: str, instruction_type: Optional[str]) -> bool:
        return self.program_name == program_name and \
               (True if instruction_type is None else self.instruction_type == instruction_type)

    def copy(self, inner: Optional[Instructions] = None) -> Instruction:
        return ParsedInstruction(
            self.program,
            inner,
            self.program_name,
            self.instruction_type,
            self.info_accounts,
            self.info_values,
            self.gen_id
        )


class Instructions:
    """
    Some helpers to work on a collection of instructions.
    """

    instructions: List[Instruction]

    _len: int

    def __init__(self, instructions: List[Instruction]):
        self.instructions = instructions

        self._len = sum(map(lambda i: len(i), self.instructions))

    def __iter__(self):
        return self.instructions.__iter__()

    def __bool__(self):
        return self._len > 0

    def __add__(self, other):
        if isinstance(other, Instructions):
            return Instructions(self.instructions + other.instructions)

        return NotImplemented

    def __len__(self):
        return self._len

    def set_ids(self, parent: Optional[str] = None) -> Instructions:
        """ Recursively generate ids for outer and any inner instructions given their index. """
        for i, instruction in enumerate(self.instructions):
            instruction.set_id(parent, i)

        return self

    @property
    def programs(self) -> Set[Account]:
        """ Program accounts across all instructions. """
        return reduce(
            lambda a, b: a | b,
            map(lambda instruction: instruction.programs, self.instructions),
            set()
        )

    def filter(self, program_name: str, instruction_type: Optional[str] = None, flatten: bool = False) -> Instructions:
        """
        Filter parsed instructions for the given program and instruction type. Outer instructions with child
        instructions of the given filter will also be returned unless flatten is True.
        """
        filtered_instructions = []
        for instruction in (self.flatten() if flatten else self):
            filtered_instruction = instruction.filter(program_name, instruction_type)
            if filtered_instruction is not None:
                filtered_instructions.append(filtered_instruction)

        return Instructions(filtered_instructions)

    def flatten(self) -> Instructions:
        """ Return a flat list of outer and inner instructions. """
        flattened = Instructions([])
        for instruction in self.instructions:
            flattened += instruction.flatten()

        return flattened
