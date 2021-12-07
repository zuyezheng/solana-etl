import gzip
import json
import time
from argparse import ArgumentParser
from pathlib import Path

from solana.rpc.api import Client


class Extract:
    """
    Extract solana blocks as JSON and dump to file.

    @author zuye.zheng
    """

    def __init__(self, endpoint: str, output_loc: str, slots_per_dir: int):
        self.endpoint = endpoint
        self.output_path = Path(output_loc)
        self.slots_per_dir = slots_per_dir

        self._client = Client(endpoint)

    def slot_path(self, slot: int) -> Path:
        """ Return the output path for a slot and ensure sub directories exists. """
        path_loc = self.output_path.joinpath(str(slot//self.slots_per_dir * self.slots_per_dir))
        path_loc.mkdir(parents=True, exist_ok=True)

        return path_loc.joinpath(f'{slot}.json.gz')

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
            if wait_duration <= max_duration:
                print(f'Waiting {wait_duration} seconds: "{e}".')

                time.sleep(wait_duration)
                result = self.execute_with_backoff(call, wait_duration * 2, max_duration)
            else:
                print(f'Max wait exceeded: "{e}".')

        return result

    def start(self, last_slot: int, first_slot: int):
        for slot in range(last_slot, first_slot - 1, -1):
            with gzip.open(self.slot_path(slot), 'w') as f:
                slot_info = self.execute_with_backoff(lambda: self._client.get_block(slot))
                if slot_info is None:
                    print(f'Error fetching info for slot {slot}.')
                else:
                    f.write(json.dumps(slot_info).encode('utf-8'))


if __name__ == '__main__':
    parser = ArgumentParser(description='Dump solana blocks counting backwards.')

    parser.add_argument(
        'output_loc',
        type=str,
        help='Where to dump the block responses.'
    )

    parser.add_argument(
        '--endpoint',
        type=str,
        help='Which net to use.',
        default='https://api.mainnet-beta.solana.com'
    )
    parser.add_argument(
        '--last_slot',
        type=int,
        help='Last slot where extraction will start.'
    )
    parser.add_argument(
        '--first_slot',
        type=int,
        help='First slot where extraction stops.',
        default=0
    )
    parser.add_argument(
        '--slots_per_dir',
        type=int,
        help='Number of slots to chunk into the same directory.',
        default=10_000
    )

    args = parser.parse_args()

    extract = Extract(args.endpoint, args.output_loc, args.slots_per_dir)
    extract.start(args.last_slot, args.first_slot)
