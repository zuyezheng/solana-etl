import json
from argparse import ArgumentParser
from typing import Set

from src.extract.Extract import Extract
from src.load.TransformTask import TransformTask
from src.transform.Block import Block


class ExtractStreaming(Extract):
    """
    Extract blocks and stream them directly through transforms and to file.

    @author zuyezheng
    """

    tasks: Set[TransformTask]

    def __init__(self, endpoint: str, output_loc: str, slots_per_dir: int, tasks: Set[TransformTask]):
        super().__init__(endpoint, output_loc, slots_per_dir)

        self.tasks = tasks

    def process_block(self, slot, block_json):
        path_base = self.output_path.joinpath(str(slot // self.slots_per_dir * self.slots_per_dir))

        try:
            block = Block(block_json, slot)

            # aggregate results and errors for each task
            for task_name in self.tasks:
                results_and_errors = self.tasks[task_name](block)
                results[task_name] = results_and_errors[0]
                errors.extend(results_and_errors[1])
        except Exception as e:
            errors.append(['json_to_blocks', block_source, str(e)])

        with gzip.open(self.slot_path(slot), 'w') as f:
            f.write(json.dumps(block_json).encode('utf-8'))


if __name__ == '__main__':
    parser = ArgumentParser(description='Extract solana blocks from rpc.')

    parser.add_argument(
        'output_loc',
        type=str,
        help='Where to dump the block responses.'
    )

    parser.add_argument(
        '--endpoint',
        type=str,
        help='Which network to use.',
        default='https://api.mainnet-beta.solana.com'
    )
    parser.add_argument(
        '--start',
        type=int,
        help='Slot to start extract.'
    )
    parser.add_argument(
        '--end',
        type=int,
        help='Slot to end extract, if less than start count down from start, if None keep counting up with backoff.',
        default=None
    )
    parser.add_argument(
        '--slots_per_file',
        type=int,
        help='Number of slots to stream to the same file.',
        default=10_000
    )

    args = parser.parse_args()

    extract = Extract(args.endpoint, args.output_loc, args.slots_per_file)
    extract.start(args.start, args.end)
