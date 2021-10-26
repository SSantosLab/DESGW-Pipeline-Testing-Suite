"""Unit tests for configure_dag.py"""

import os
import sys
import unittest

import pandas as pd

sys.path.append('..')
import configure_dag


class TestConfigureDAG(unittest.TestCase):
    """Validate configure_dag.py functionalities."""

    def test_get_exposure_info(self):
        """Check return type."""
        ra, dec = 60., -30.
        exposure_df = configure_dag.get_exposure_info(ra, dec)
        self.assertTrue(isinstance(exposure_df, pd.DataFrame))

    def test_write_dag_rc(self):
        """Check that the outfile exisits."""
        outfile = "test_write_dag.rc"
        empty_df = pd.DataFrame()
        configure_dag.write_dag_rc(empty_df, outfile)
        self.assertTrue(os.path.exists(outfile))

        # Clean up after test runs.
        if os.path.exists(outfile):
            os.system(f"rm {outfile}")


if __name__ == "__main__":
    unittest.main()