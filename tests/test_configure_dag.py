"""Unit tests for configure_dag.py"""

import os
import sys
import unittest

import pandas as pd

sys.path.append('..')
import configure_dag


class TestConfigureDAG(unittest.TestCase):
    """Validate configure_dag.py functionalities."""
    def setUp(self):
        # Load a df of sample exposures.
        self.test_exposure_df = pd.read_csv(
            'sample_exposures.tab',
            delim_whitespace=True,
            header=None,
            names=[
                'EXPNUM', 'NITE', 'MJD', 'RA', 'DEC', 'BAND', 'EXPTIME', 
                'PROPID', 'OBJECT', 'TEFF', 'COMMENT', 'SEARCH',
            ]
        )

    def test_get_exposure_info(self):
        """Check return type."""
        ra, dec = 60., -30.
        exposure_df = configure_dag.get_exposure_info(ra, dec)
        self.assertTrue(isinstance(exposure_df, pd.DataFrame))

    def test_write_dag_rc(self):
        """Check that the outfile written correctly."""
        def _get_value(parameter: str, lines: list) -> str:
            """Get the value of a parameter in the configuration file.

            Args:
              parameter (str): The name of the parameter to obtain.
              lines (list): Output of file.readlines().
            
            Returns:
              The value of the parameter in the configuration file as a string.

            Raises:
              KeyError if the parameter is not in the configuration file.
            """
            param_lines = [x for x in lines if x.startswith(parameter)]
            if len(param_lines) == 0:
                raise KeyError(f"{parameter} not found in configuration file.")

            return param_lines[0].split('=')[-1].strip()


        outfile = "test_write_dag.rc"
        configure_dag.write_dag_rc(self.test_exposure_df, outfile)

        # Test that the outfile was made.
        self.assertTrue(os.path.exists(outfile))

        # Open the file to inspect it.
        with open(outfile) as f:
            lines = f.readlines()

            # Test that the file is the correct length.
            self.assertEqual(len(lines), 26)

            # Test that MIN_NITE, MAX_NITE, and TWINDOW have correct values.
            expected_time_info = configure_dag._get_time_boundaries(
                self.test_exposure_df)
            min_nite = int(_get_value('MIN_NITE', lines))
            max_nite = int(_get_value('MAX_NITE', lines))
            twindow = float(_get_value('TWINDOW', lines))
            self.assertEqual(min_nite, expected_time_info.min_nite)
            self.assertEqual(max_nite, expected_time_info.max_nite)
            self.assertAlmostEqual(twindow, expected_time_info.twindow)

        # Clean up after test runs.
        if os.path.exists(outfile):
            os.system(f"rm {outfile}")

    def test_get_time_boundaries(self):
        """Check that twindow, min_nite, and max_nite are correct."""
        # Verify return type.
        time_info = configure_dag._get_time_boundaries(self.test_exposure_df)
        self.assertTrue(isinstance(time_info, configure_dag.TimeInfo))

        # Check values.
        self.assertEqual(time_info.min_nite, 20100101)
        self.assertEqual(time_info.max_nite, 21000101)
        self.assertAlmostEqual(time_info.twindow, 2.0, places=1)


if __name__ == "__main__":
    unittest.main()