import gzip
import json
from argparse import ArgumentParser
from typing import Dict

from src.extract.Extract import Extract


class ExtractBatch(Extract):
    """
    Extract blocks as their raw responses for later batch processing.

    @author zuyezheng
    """

    def process_block(self, slot: int, block_json: Dict):
        # create subdirectories for each chunk of blocks
        path_loc = self.output_path.joinpath(str(slot // self.slots_per_dir * self.slots_per_dir))
        path_loc.mkdir(parents=True, exist_ok=True)
        path_loc = path_loc.joinpath(f'{slot}.json.gz')

        with gzip.open(path_loc, 'w') as f:
            f.write(json.dumps(block_json).encode('utf-8'))


def main():
    parser = ArgumentParser(description='Extract solana blocks from rpc.')

    parser.add_argument(
        'output_loc', type=str, help='Directory to dump block responses.'
    )
    parser.add_argument(
        '--endpoint', type=str, help='Which network to use.', default='https://api.mainnet-beta.solana.com'
    )
    parser.add_argument(
        '--start', type=int, help='Slot to start extract.'
    )
    parser.add_argument(
        '--end',
        type=int,
        help='Slot to end extract, if less than start count down from start, if None keep counting up with backoff.',
        default=None
    )
    parser.add_argument(
        '--slots_per_dir',  type=int, help='Number of slots to stream to the same file.', default=10_000
    )

    args = parser.parse_args()

    extract = ExtractBatch(args.endpoint, args.output_loc, args.slots_per_dir)
    extract.start(args.start, args.end)


if __name__ == '__main__':
    main()
