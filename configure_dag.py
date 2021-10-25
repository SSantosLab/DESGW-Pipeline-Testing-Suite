"""A module to configure the DAGMaker RC file.

The primary use of the functions in this script is to set up the DAGMaker.rc
file for monthly end-to-end tests. The steps are:
    (1) Get the exposure info for a given RA and DEC, then
    (2) Write the DAGMaker.rc file based on the input exposures.
"""

import logging
import random
import sys

import pandas as pd

import utils


### Main functions.

#@utils.log_start_and_finish
def get_exposure_info(ra: float, dec: float) -> pd.DataFrame:
    """Create an exposure info df for all exposures in a pointing.

    Args:
      ra (float): The right ascension of the pointing.
      dec (float): The declination of the pointing.

    Returns:
      A DataFrame containing the exposure information.
    """
    # TODO(@Alyssa)
    return pd.DataFrame()

#@utils.log_start_and_finish
def write_dag_rc(exposure_df: pd.DataFrame, outfile: str):
    """Write a DAGMaker.rc file based on the exposure information.

    Args:
      exposure_df (pd.DataFrame): A DataFrame containing the exposure info.
      outfile (str): Name of outfile to create containing DAG.
    """
    # TODO(@Rob)
    pass

### Helper functions.



### Runtime behavior.
if __name__ == "__main__":
    # Setup logging.
    log_file = "configure_dag.log"  # TODO(@Rob): decide on default filename.
    utils._setup_logging(log_file)

    # Choose pointing.
    ra = random.uniform(0.0, 360.0 - 1.e-5)
    dec = random.uniform(-90.0, 30.0)  # +30 is the upper limit for DECam.
    logging.info(f"ra = {ra}")
    logging.info(f"dec= {dec}")

    # Get exposures.
    outfile_name = "dagmaker.rc"  # TODO(@Rob): decide on default filename.
    exposure_df = get_exposure_info(ra, dec)
    logging.info("get_exposure_info output:")
    logging.info(exposure_df)

    # Create DAGMaker rc.
    write_dag_rc(exposure_df, outfile_name)

    logging.debug("Program Completed.")



