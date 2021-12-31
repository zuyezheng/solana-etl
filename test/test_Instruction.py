import unittest
from pathlib import Path

from src.Block import Block
from src.Transaction import Transaction


class BlockTest(unittest.TestCase):
    _interesting_transaction: Transaction

    @classmethod
    def setUpClass(cls):
        block = Block.open(Path(f'test/resources/110130000.json.gz'))
        cls._interesting_transaction = block.find_transaction(
            '2XMqtpXpp83pupsM5iiie2s69iRTHrV6oA6zxDTY9hRC4M2Rr9Yh5knSkBZbk22Wt7Qv88akacJifnaX6oL5ncqS'
        )

    def test_properties(self):
        self.assertEqual(
            21,
            self._interesting_transaction.instructions.size,
            'Size should be count of outer and inner instructions.'
        )

        self.assertEqual(
            {
                '11111111111111111111111111111111',
                'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
                'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ',
                'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            },
            set(map(lambda a: a.key, self._interesting_transaction.instructions.programs)),
            'Program keys should include those from inner and outer instructions.'
        )

    def test_flatten(self):
        flattened = self._interesting_transaction.instructions.flatten()

        self.assertEqual(21, flattened.size, 'Flattened including inner should have the same number of instructions.')

        self.assertEqual(
            [
                '0',
                '1',
                '2', '2.0', '2.1', '2.2', '2.3',
                '3',
                '4', '4.0', '4.1', '4.2', '4.3', '4.4', '4.5', '4.6', '4.7', '4.8', '4.9', '4.10', '4.11'
            ],
            list(map(
                lambda instruction: instruction.gen_id,
                flattened
            ))
        )

    def test_filter(self):
        self.assertEqual(
            [
                '0',
                '2', '2.0', '2.1', '2.2',
                '4', '4.0', '4.2', '4.3', '4.4', '4.6', '4.7', '4.8'
            ],
            list(map(
                lambda instruction: instruction.gen_id,
                self._interesting_transaction.instructions.filter('system').flatten()
            ))
        )

        filtered = self._interesting_transaction.instructions.filter('system', 'transfer')
        self.assertEqual(
            [
                '2', '2.0',
                '4', '4.0', '4.2', '4.6'
            ],
            list(map(lambda instruction: instruction.gen_id, filtered.flatten()))
        )
        self.assertEqual(6, filtered.size)

        # flatten before filtering will exclude outer instructions
        filtered = self._interesting_transaction.instructions.filter('system', 'transfer', flatten=True)
        self.assertEqual(
            [
                '2.0',
                '4.0', '4.2', '4.6'
            ],
            list(map(lambda instruction: instruction.gen_id, filtered.flatten()))
        )
        self.assertEqual(4, filtered.size)
