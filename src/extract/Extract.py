import itertools
import time
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

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


@dataclass
class TimedResponse:
    response: any
    call_time: float
    total_time: float = -1

    def with_total(self, total: float):
        return TimedResponse(self.response, self.call_time, total)


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
    ) -> TimedResponse:
        start = time.perf_counter()
        response = TimedResponse(None, -1)

        try:
            call_start = time.perf_counter()
            call_response = call()
            response = TimedResponse(call_response, time.perf_counter() - call_start)
        except Exception as e:
            retryable = True
            if isinstance(e, BlockException):
                retryable = e.should_retry()

            if retryable and wait_duration <= max_duration:
                print(f'Waiting {wait_duration} seconds: "{e}".')

                time.sleep(wait_duration)
                response = self.execute_with_backoff(call, wait_duration * 2, max_duration)
            else:
                print(f'Max wait exceeded: "{e}".')

        return response.with_total(time.perf_counter() - start)

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

        num_blocks = 0
        call_time = 0
        call_time_with_wait = 0
        process_time = 0

        for slot in get_slots():
            timed_response = self.execute_with_backoff(lambda: self.get_block(slot))
            if timed_response.response is None:
                print(f'Error fetching info for slot {slot}.')
            else:
                call_time += timed_response.call_time
                call_time_with_wait += timed_response.total_time

                start = time.perf_counter()
                self.process_block(slot, timed_response.response)
                process_time += time.perf_counter() - start

                num_blocks += 1

            if num_blocks % 60 == 0:
                print(f'Extracted {num_blocks} blocks ending on {slot} with average times: '
                      f'call: {call_time/num_blocks:.2f}s, '
                      f'call with wait: {call_time_with_wait/num_blocks:.2f}s, '
                      f'process: {process_time/num_blocks:.2f}s.')

                num_blocks = 0
                call_time = 0
                call_time_with_wait = 0
                process_time = 0


    @abstractmethod
    def process_block(self, slot: int, block_json: Dict):
        raise NotImplemented


