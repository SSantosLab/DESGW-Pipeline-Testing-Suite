# Imports
import numpy as np
import pandas as pd
import psycopg2
import subprocess
import os
import time
from subprocess import Popen, PIPE
import argparse
import sys

# Arguments
parser = argparse.ArgumentParser()
parser.add_argument('--exp_list', type=str)
parser.add_argument('--coadd', default=False)
args = parser.parse_args()

# Function
def EXPlist(explist):
    
    e = open(explist,'r')
    el = e.readlines()
    elist = [exp.strip() for exp in el]
    e.close()

    return elist

def getCoadd(eList):
    
    df = pd.DataFrame(columns=['exposure', 'nite', 'radeg', 'decdeg']) # Create empty dataframe to which to append query results
    
    # Connect to database
    conn =  psycopg2.connect(database='decam_prd',user='decam_reader',host='des61.fnal.gov',port=5443)
    
    for exp in eList:
        
        query = """SELECT id as EXPOSURE,
        TO_CHAR(date - '12 hours'::INTERVAL, 'YYYYMMDD') AS NITE,
        ra AS RADEG,
        declination AS DECDEG,
        filter as BAND
        FROM exposure.exposure
        WHERE id ="""+exp+""" ORDER BY id""" 

        exposure_data = pd.read_sql(query, conn)#,index_col ="exposure")
        
        df = pd.concat([df,exposure_data]) # Add exposure_data dataframe to overhead dataframe
        
    conn.close()
    df = df.set_index('exposure', drop = True)   
    
    coadd_explist = [] # List for lists of coadd sets
    temp_eList = eList[:] # A copy of the list of exposures to be slowly dismantled as coadd sets are found
    temp_eList = [int(e) for e in temp_eList]
    
    for exposure in temp_eList:
        exp_ra = df.loc[int(exposure)]['radeg']
        exp_dec = df.loc[int(exposure)]['decdeg']
        exp_nite = df.loc[int(exposure)]['nite']
        exp_band = df.loc[int(exposure)]['band']
        
        # Winnow df until we just have one that has coadds for given exp
        stage1_df = df[df['radeg'] == exp_ra]
        stage2_df = stage1_df[stage1_df['decdeg'] == exp_dec]
        stage3_df = stage2_df[stage2_df['nite'] == exp_nite]
        stage4_df = stage3_df[stage3_df['band'] == exp_band]
        
        coadds = stage3_df.index.values.tolist() # Select coadd exposures as a list
        coadd_explist.append(coadds)
        
        for coadd in coadds:
            temp_eList.remove(coadd)
    
    coadd_str_list = []
    for coadd_set in coadd_explist:
        coadd_str = ''
        for exp in coadd_set:
            if exp == coadd_set[-1]:
                coadd_str += str(exp)
            else:
                coadd_str += str(exp) + ' '
        coadd_str_list.append(coadd_str)


    return coadd_str_list


# Script
exposures = EXPlist(args.exp_list)

if args.coadd:
    exposures = getCoadd(exposures)
    
## Get expsure groups
len_exps = len(exposures)
if args.coadd:
    print("The number of coadd sets is " + str(len_exps) + ".")
else:
    print("The number of exposures is "+ str(len_exps) + ".")
last_set_len = len_exps % 5 # For submissions of 4 DAGmaker run at a time, the size of the last set of DAGmaker runs
num_full_sets = len_exps // 5 # Number of DAGmaker sets of 4 runs
if last_set_len > 0:
    number_o_sets = num_full_sets + 1
    print("The number of threaded DAGmaker runs will be "+ str(number_o_sets)+": "+str(num_full_sets) + " sets of 5 DAGmaker runs and 1 set of "+ str(last_set_len) + " DAGmaker run(s).")
else:
    number_o_sets = num_full_sets
    print("The number of threaded DAGmaker runs will be "+ str(number_o_sets)+": "+str(num_full_sets) + " sets of 5 DAGmaker runs.")    

## Run DAGMaker
start_index = 0
for i in range(num_full_sets):

    exp1_index = start_index
    exp2_index = start_index + 1
    exp3_index = start_index + 2
    exp4_index = start_index + 3
    exp5_index = start_index + 4

    start_index += 5
    
    cmd = ['./DAGMaker.sh ' + exposures[exp1_index]]
    process1 = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Running " + cmd[0])
    cmd = ['./DAGMaker.sh ' + exposures[exp2_index]]
    process2 = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Running " + cmd[0])
    cmd = ['./DAGMaker.sh ' + exposures[exp3_index]]
    process3 = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Running " + cmd[0])
    cmd = ['./DAGMaker.sh ' + exposures[exp4_index]]
    process4 = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Running " + cmd[0])
    cmd = ['./DAGMaker.sh ' + exposures[exp5_index]]
    process5 = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("Running " + cmd[0])

    jobsub_info = []
    rel_exps = []
    for exposure, process in zip(exposures[exp1_index : exp5_index +1],[process1, process2, process3, process4, process5]):
        stdout, stderr = process.communicate()
        f = open('dagmaker_'+exposure+'.out', 'w')
        f.write(stdout)
        if stderr != None:
            f.write(stderr)
        f.close() 
        jsub = subprocess.check_output(['tail', '-3', 'dagmaker_'+exposure+'.out'])
        jsub = jsub.split(b'\n')
    
        try:

            if jsub[0].decode('ascii') == 'NO TEMPLATE IMAGES, DIFFIMG WILL FAIL':
                print(jsub[0].decode('ascii'))
                continue
            else:
                print('Jobsub command: ' + jsub[-2].decode('ascii'))
                jobsub_info.append(jsub[-2].decode('ascii'))
                rel_exps.append(exposure)

        except(UnicodeDecodeError, AttributeError):

            if jsub[0] == 'NO TEMPLATE IMAGES, DIFFIMG WILL FAIL':
                print(jsub[0])
                continue
            else:
                print('Jobsub command: ' + jsub[-2])
                jobsub_info.append(jsub[-2])
                rel_exps.append(exposure)
        
    for jobsub_datum in jobsub_info:
        if jobsub_datum.split()[0] != 'jobsub_submit_dag':
            f = open('Problematic_DAGmaker_Outputs.txt', 'a+')
            f.write(str(exposure) + '\n')
            f.close()
        else:
            cmd = [jobsub_datum]
            process = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            exposure = rel_exps[jobsub_info.index(jobsub_datum)]
            f = open('jobsub_'+exposure+'.out', 'w')
            f.write(stdout)
            if stderr != None:
                f.write(stderr)
                print("Something went wrong with submitting the job for " + exposure + ".")
            f.close()

exp_ = []
proc_ = []
jobsub_info = []
rel_exps = []
for i in range(last_set_len):
    exp_index = -1 - i 
    cmd = ['./DAGMaker.sh ' + exposures[exp_index]]
    process = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print('Running ' + cmd[0])
    
    exp_.append(exposures[exp_index])
    proc_.append(process)
    
for exposure, process in zip(exp_,proc_):
    stdout, stderr = process.communicate()
    f = open('dagmaker_'+exposure+'.out', 'w')
    f.write(stdout)
    if stderr != None:
        f.write(stderr)
    f.close()
    jsub = subprocess.check_output(['tail', '-3', 'dagmaker_'+exposure+'.out'])
    jsub = jsub.split(b'\n')
    
    try:

        if jsub[0].decode('ascii') == 'NO TEMPLATE IMAGES, DIFFIMG WILL FAIL':
            print(jsub[0].decode('ascii'))
            continue
        else:
            print('Jobsub command: ' + jsub[-2].decode('ascii'))
            jobsub_info.append(jsub[-2].decode('ascii'))
            rel_exps.append(exposure)
            
    except(UnicodeDecodeError, AttributeError):
        
        if jsub[0] == 'NO TEMPLATE IMAGES, DIFFIMG WILL FAIL':
            print(jsub[0])
            continue
        else:
            print('Jobsub command: ' + jsub[-2])
            jobsub_info.append(jsub[-2])
            rel_exps.append(exposure)
    
for jobsub_datum in jobsub_info:
    if jobsub_datum.split()[0] != 'jobsub_submit_dag':
        f = open('Problematic_DAGmaker_Outputs.txt', 'a+')
        f.write(str(exposure) + '\n')
        f.close()
    else:
        cmd = [jobsub_datum]
        process = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = process.communicate()
        exposure = rel_exps[jobsub_info.index(jobsub_datum)]
        f = open('jobsub_'+exposure+'.out', 'w')
        f.write(stdout)
        if stderr != None:
            f.write(stderr)
            print("Something went wrong with submitting the job for " + exposure + ".")
        f.close()
