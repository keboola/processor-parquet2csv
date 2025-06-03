import os
import unittest
import logging

from datadirtest import DataDirTester


class TestComponent(unittest.TestCase):
    def test_functional(self):
        logging.info("Running functional tests")
        os.environ["KBC_STACKID"] = "connection.keboola.com"
        os.environ["KBC_PROJECT_FEATURE_GATES"] = "queuev2"
        functional_tests = DataDirTester()
        functional_tests.run()

    def test_functional_dtypes(self):
        logging.info("Running functional tests with dtypes")
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "authoritative"
        functional_tests = DataDirTester(data_dir="./tests/functional_dtypes")
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
