"""A module to configure the DAGMaker RC file.

The primary use of the functions in this script is to set up the DAGMaker.rc
file for monthly end-to-end tests. The steps are:
    (1) Get the exposure info for a given RA and DEC, then
    (2) Write the DAGMaker.rc file based on the input exposures.
"""

from dataclasses import dataclass
import datetime
import logging
import random
import sys

import pandas as pd

import utils


### Main functions.

@utils.log_start_and_finish
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

@utils.log_start_and_finish
def write_dag_rc(
  exposure_df: pd.DataFrame, season: int, outfile: str = 'dagmaker.rc'):
    """Write a DAGMaker.rc file based on the exposure information.

    Args:
      exposure_df (pd.DataFrame): A DataFrame containing the exposure info.
      season (int): The season to use for the database.
      outfile (str, default='dagmaker.rc'): Name of outfile.
    """
    time_info = _get_time_boundaries(exposure_df)

    dag_info = f"""
RNUM=4
PNUM=7
SEASON={season}
DIFFIMG_EUPS_VERSION=gw7
WRITEDB=on
RM_MYTEMP=true
JOBSUB_OPTS="--memory=2500MB --disk=70GB --cpu=1 --expected-lifetime=5h --email-to=alyssag94@brandeis.edu --disk=70GB"
JOBSUB_OPTS_SE="--memory=3600MB --disk=100GB --cpu=1 --expected-lifetime=5h"
RESOURCES="DEDICATED,OPPORTUNISTIC,OFFSITE"
IGNORECALIB=true
DESTCACHE=persistent
SEARCH_OPTS="-C"
TEMP_OPTS="-C -t"
SCHEMA="gw"
TEFF_CUT_g=0.3
TEFF_CUT_i=0.3
TEFF_CUT_r=0.3
TEFF_CUT_Y=0.3
TEFF_CUT_z=0.3
TEFF_CUT_u=0.3
TWINDOW={time_info.twindow}
MIN_NITE={time_info.min_nite}
MAX_NITE={time_info.max_nite}
SKIP_INCOMPLETE_SE=false
DO_HEADER_CHECK=1
"""
    with open(outfile, 'w+') as f:
        f.write(dag_info)

### Helper functions.
def _get_season(date: datetime.date) -> int:
  """Convert a date to a season number.

  The season numbers follow the form YYMM, so this function formats a date to
  have the season format.

  Args:
    date: A datetime.date object.

  Returns:
    The date in YYMM format as an int.
  """
  return int((date.year - 2000) * 100 + date.month)

@dataclass
class TimeInfo:
    min_nite: int
    max_nite: int
    twindow: float

@utils.log_start_and_finish
def _get_time_boundaries(exposure_df: pd.DataFrame) -> TimeInfo:
    """Determine the time information for the DAGMaker configuration file.

    The configuration file needs appropriate values for MIN_NITE, MAX_NITE,
    and TWINDOW based on the chosen exposures. This function determines those
    quantities.

    We set MIN_NITE and MAX_NITE to extreme values to include all available
    templates, but this behavior can be changed later if desired. TWINDOW is
    set to the time difference in days between the first and last search
    exposure, and a small padding of 2 days is added.

    Args:
      exposure_df (pd.DataFrame): A DataFrame containing the exposure info.

    Returns:
      a TimeInfo object with min_nite, max_nite, and twindow attributes.
    """
    min_nite = 20100101
    max_nite = 21000101

    mjds = exposure_df['MJD'].values.astype(float)
    mask = exposure_df['SEARCH'].values.astype(str) == "True"
    twindow = mjds[mask].max() - mjds[mask].min() + 2.0

    logging.info(f"min_nite = {min_nite}")
    logging.info(f"max_nite = {max_nite}")
    logging.info(f"twindow = {twindow}")

    return TimeInfo(min_nite=min_nite, max_nite=max_nite, twindow=twindow)

### Runtime behavior.
if __name__ == "__main__":
    # Setup logging.
    log_file = "configure_dag.log"  # TODO(@Rob): decide on default filename.
    utils._setup_logging(log_file)

    # Choose season.
    season = _get_season(datetime.date.today())

    # Choose pointing.
    ra = random.uniform(0.0, 360.0 - 1.e-5)
    dec = random.uniform(-90.0, 30.0)  # +30 is the upper limit for DECam.
    logging.info(f"ra = {ra}")
    logging.info(f"dec= {dec}")

    # Get exposures.
    exposure_df = get_exposure_info(ra, dec)
    logging.info("get_exposure_info output:")
    logging.info(exposure_df)

    # Create DAGMaker rc.
    write_dag_rc(exposure_df, season)

    logging.debug("Program Completed.")



