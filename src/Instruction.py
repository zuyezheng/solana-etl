from __future__ import annotations

from functools import reduce
from typing import Dict, List, Set

from src.Account import Account
from src.Accounts import Accounts


class Instruction:
    """
    Make sense of partially and fully parsed instructions and put together inner with outer instructions from different
    parts of the transaction.

    @author zuyezheng
    """
    program_key: str
    inner_instructions: Instructions
    size: int

    # all accounts used in this and inner instructions
    accounts: Set[Account]
    # all program accounts in this and any inner instructions
    programs: Set[Account]

    @staticmethod
    def from_json(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: List[Instruction] = None
    ) -> Instruction:
        if 'parsed' in json_data:
            return ParsedInstruction.from_json(all_accounts, json_data, inner_instructions)
        else:
            return PartiallyParsedInstruction.from_json(all_accounts, json_data, inner_instructions)

    def __init__(
        self,
        # all accounts in the transaction
        all_accounts: Accounts,
        # accounts used in this instruction
        accounts: Set[Account],
        # program key
        program_key: str,
        inner_instructions: List[Instruction] = None
    ):
        self.program_key = program_key
        self.inner_instructions = Instructions([] if inner_instructions is None else inner_instructions)
        self.size = self.inner_instructions.size + 1

        self.accounts = accounts
        self.programs = all_accounts.from_keys([program_key]) | self.inner_instructions.programs


class PartiallyParsedInstruction(Instruction):
    """ Instruction where we only know about the accounts and program involved and not the actual operations. """
    data: str

    @staticmethod
    def from_json(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: List[Instruction] = None
    ) -> PartiallyParsedInstruction:
        return PartiallyParsedInstruction(
            all_accounts,
            json_data['programId'],
            inner_instructions,
            json_data['accounts'],
            json_data['data']
        )

    def __init__(
        self,
        all_accounts: Accounts,
        program_key: str,
        inner_instructions: List[Instruction],
        account_keys: List[str],
        data: str
    ):
        super().__init__(all_accounts, all_accounts.from_keys(account_keys), program_key, inner_instructions)

        self.data = data


class ParsedInstruction(Instruction):
    program_name: str
    instruction_type: str
    info_accounts: Dict[str, Account]
    info_values: Dict[str, any]

    @staticmethod
    def from_json(
        all_accounts: Accounts,
        json_data: Dict[str, any],
        inner_instructions: List[Instruction] = None
    ) -> ParsedInstruction:
        return ParsedInstruction(
            all_accounts,
            json_data['programId'],
            inner_instructions,
            json_data['program'],
            json_data['parsed']['type'],
            json_data['parsed']['info']
        )

    def __init__(
        self,
        all_accounts: Accounts,
        program_key: str,
        inner_instructions: List[Instruction],
        program_name: str,
        instruction_type: str,
        info: Dict[str, any]
    ):
        # need to do some work to figure out which of the arguments are account keys
        info_accounts = {}
        info_values = {}
        for info_key, info_value in info.items():
            # see if the value is an account key or another instruction argument, probably should look deeper into
            # nested object arguments, but seems to cover all current use cases
            account = all_accounts.get(info_value) if isinstance(info_value, str) else None

            if account is None:
                info_values[info_key] = info_value
            else:
                info_accounts[info_key] = account

        super().__init__(all_accounts, set(info_accounts.values()), program_key, inner_instructions)

        self.program_name = program_name
        self.instruction_type = instruction_type
        self.info_accounts = info_accounts
        self.info_values = info_values


class Instructions:
    """
    Some helpers to work on a collection of instructions.
    """
    instructions: List[Instruction]
    size: int

    def __init__(self, instructions: List[Instruction]):
        self.instructions = instructions

        self.size = sum(map(lambda i: i.size, self.instructions))

    def __iter__(self):
        return self.instructions.__iter__()

    @property
    def programs(self) -> Set[Account]:
        """ Program accounts across all instructions. """
        return reduce(
            lambda a, b: a | b,
            map(lambda instruction: instruction.programs, self.instructions),
            set()
        )
