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
import more_itertools as mit
import numpy as np
import psycopg2


### Main functions.

@utils.log_start_and_finish
def get_exposure_info(ra: float, dec: float, minexp=None, maxexp=None) -> pd.DataFrame:
    """Create an exposure info df for all exposures in a pointing.

    Args:
      ra (float): The right ascension of the pointing.
      dec (float): The declination of the pointing.

    Returns:
      A DataFrame containing the exposure information.
    """
    minra = ra - 5.0
    maxra = ra + 5.0
    mindec = dec - 5.0
    maxdec = dec + 5.0

    # for quick testing, use know expnumns
    # get all the info that would normally be in exposures.list
    if minexp is not None and maxexp is not None:
        query = """                                                                           
            SELECT id as EXPNUM,                                                              
            TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS NITE,                         
            EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) AS MJD_OBS,          
                ra AS RADEG,                                                                  
                declination AS DECDEG,                                                        
                filter AS BAND,                                                               
                exptime AS EXPTIME,                                                           
                propid AS PROPID,                                                             
                flavor AS OBSTYPE,                                                            
                qc_teff as TEFF,                                                              
                object as OBJECT                                                              
            FROM exposure.exposure                                                            
            WHERE flavor='object' and exptime>29.999 and RA is not NULL and                   
                id>="""+str(minexp)+"""and id<="""+str(maxexp)+"""                   
            ORDER BY id"""
        conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des61.fnal.gov',
                           port=5443)
        allexps = pd.read_sql(query, conn)
        search = ['True'] * len(allexps.index)
        allexps['SEARCH'] = search
        return allexps

    else:
        # using ra and dec to create a square and query from there
        query = """ SELECT id as EXPNUM,
                    TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS NITE,
                    EXTRACT(EPOCH FROM date - '1858-11-17T00:00:00Z')/(24*60*60) AS MJD_OBS,
                        ra AS RADEG,
                        declination AS DECDEG,
                        filter AS BAND,                                                                
                        exptime AS EXPTIME,                                                            
                        propid AS PROPID,                                                              
                        flavor AS OBSTYPE,                                                             
                        qc_teff as TEFF,                                                               
                        object as OBJECT 
                    FROM exposure.exposure                                                             
                    WHERE flavor='object' and exptime>29.999  
                        and RA >="""+str(minra)+""" and RA <= """+str(maxra)+"""  
                        and DECLINATION >="""+str(mindec)+""" and DECLINATION <="""+str(maxdec)+""" 
                   ORDER BY id"""
        conn =  psycopg2.connect(database='decam_prd',
                           user='decam_reader',
                           host='des61.fnal.gov',
                           port=5443)
        allexps = pd.read_sql(query, conn)

        # get only exposures with appropriate teff and exposure lenghts, and filters
        df = allexps[(allexps['exptime']<=200) & (allexps['exptime']>=30)
                     & (allexps['teff'] >=0.05) 
                     & (allexps.band.isin(['g', 'r', 'i', 'z']))].copy().reset_index(drop=True)
        print(len(df))
        
        # decide which exposure will be search and temps                                   
        gp = df.groupby('nite')
        if gp.ngroups < 2:
            # only one nite available, would this even work?
            print('ONLY ONE NIGHT OF EXPOSURES AVAILABLE. WILL RUN, PIPELINE WILL LIKELY FAIL')
            print('CONSIDER RERUNNING')
            df = pd.read_sql(query, conn)
            search = [True] * len(df.index)

        elif gp.ngroups == 2:
            # use group with most recent exposures as search
            srch_nites = max(gp.groups.keys())
            search = []
            for n in df['nite']:
                if int(n) in srch_nites:
                    search.append(True)
                else:
                    search.append(False)

        else:
            # ideally want nights with multiple exposures per night
            sizes = dict(df.groupby('nite').size())
            src = {}
            for key, val in sizes.items():
                if val > 2: # 4 exposures per night is arbitrary 
                    src[int(key)] = val
            cons = [list(group) for group in mit.consecutive_groups(sorted(src.keys()))] #groups by consec. nites
            
            # pick the most recent expousres as search
            try:
                srch_nites = max([nite for nite in cons if len(nite)>=2])
            except: #nothing with consecutive nights
                srch_nites = max(cons)

            # create the search column
            search = []
            i = 0
            for n in df['nite']:
                if int(n) in srch_nites:
                    i += 1
                    if i <= 20:
                        search.append(True)
                    else:
                        search.append(False)
                else:
                    search.append(False)

        df['SEARCH'] = search
        return df
        

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

    mjds = exposure_df['mjd_obs'].values.astype(float)
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
    exposure_df.to_csv(str(season)+'exposures.csv', index=False)
    np.savetxt(str(season)+'exposures.list',exposure_df['expnum'].values[exposure_df['SEARCH'].values], fmt='%d')
    logging.info("get_exposure_info output:")
    logging.info(exposure_df)

    # Create DAGMaker rc.
    write_dag_rc(exposure_df, season)

    logging.debug("Program Completed.")



