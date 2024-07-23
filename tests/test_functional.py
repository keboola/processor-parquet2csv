import os
import unittest

from datadirtest import DataDirTester


class TestComponent(unittest.TestCase):

    def test_functional(self):
        functional_tests = DataDirTester()
        functional_tests.run()

    def test_functional_dtypes(self):
        os.environ['KBC_DATA_TYPE_SUPPORT'] = 'authoritative'
        functional_tests = DataDirTester(data_dir='./tests/functional_dtypes')
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
