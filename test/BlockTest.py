import unittest
from pathlib import Path

from src.Block import Block


class BlockTest(unittest.TestCase):

    def test_transactions(self):
        total_transactions = 0
        total_transactions_more_than_fee = 0

        block = Block.open(Path(f'test/resources/110130000.json.gz'))
        self.assertTrue(block.has_transactions())

        if not block.missing and len(block.transactions()) > 0:
            transactions_more_than_fee = 0
            for transaction in block.transactions():
                if transaction.total_balance_change() != transaction.fee():
                    transactions_more_than_fee += 1
                    print(transaction.signature(), transaction.total_balance_change()/2000000000)

            print(transactions_more_than_fee/len(block.transactions()))

            total_transactions += len(block.transactions())
            total_transactions_more_than_fee += transactions_more_than_fee

        print(total_transactions, total_transactions_more_than_fee, total_transactions_more_than_fee / total_transactions * 100)

    def test_out(self):
        block = Block.open(Path(f'test/resources/110130000.json.gz'))
        for transaction in block.transactions():
            outs = 0
            for key, value in transaction.balance_changes().items():
                if value < 0:
                    outs += 1

            self.assertEqual(1, outs, transaction.signature())

    def test_instructions(self):
        block = Block.open(Path(f'test/resources/110130000.json.gz'))
        print(block.find_transaction('2XMqtpXpp83pupsM5iiie2s69iRTHrV6oA6zxDTY9hRC4M2Rr9Yh5knSkBZbk22Wt7Qv88akacJifnaX6oL5ncqS').instructions().count)
