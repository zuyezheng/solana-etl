import itertools
import time
from abc import abstractmethod
from pathlib import Path

from solana.rpc.api import Client


class BlockException(Exception):

    def __init__(self, error_json):
        self.error_json = error_json
        super().__init__(f'Error code {self.error_json["code"]}: {self.error_json["message"]}')

    def should_retry(self):
        if self.error_json['code'] == -32004:
            # block not yet available, should wait for it
            return True

        return False


class Extract:
    """
    Extract solana blocks as gzipped JSON and dump to file.

    @author zuyezheng
    """

    def __init__(self, endpoint: str, output_loc: str, slots_per_dir: int):
        self.endpoint = endpoint
        self.output_path = Path(output_loc)
        self.slots_per_dir = slots_per_dir

        self._client = Client(endpoint)

    def execute_with_backoff(
        self,
        # lambda to execute
        call,
        # duration of wait if exception is thrown in call
        wait_duration: int = 5,
        # wait will be doubled each time until max is exceeded
        max_duration: int = 60
    ):
        result = None
        try:
            result = call()
        except Exception as e:
            retryable = True
            if isinstance(e, BlockException):
                retryable = e.should_retry()

            if retryable and wait_duration <= max_duration:
                print(f'Waiting {wait_duration} seconds: "{e}".')

                time.sleep(wait_duration)
                result = self.execute_with_backoff(call, wait_duration * 2, max_duration)
            else:
                print(f'Max wait exceeded: "{e}".')

        return result

    def get_block(self, slot: int):
        block = self._client.get_block(slot, 'jsonParsed')
        if 'error' in block:
            raise BlockException(block['error'])

        return block

    def start(self, start: int, end: int):
        def get_slots():
            if end is None:
                return itertools.count(start)
            elif end < start:
                return range(start, end - 1, -1)
            else:
                return range(start, end + 1)

        for slot in get_slots():
            block_json = self.execute_with_backoff(lambda: self.get_block(slot))
            if block_json is None:
                print(f'Error fetching info for slot {slot}.')
            else:
                self.process_block(slot, block_json)

    @abstractmethod
    def process_block(self, slot, block_json):
        raise NotImplemented


