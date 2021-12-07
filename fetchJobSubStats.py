import glob
import pickle
import pandas as pd
import numpy as np
import configparser
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--season', type=str)
parser.add_argument('--exp_list', type=str)
args = parser.parse_args()

season = str(args.season)

cmd = ['python get_full_exp_info.py --exp_list ' + args.exp_list]
process = subprocess.Popen(cmd, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

exp_details = pd.read_csv('exp_list_full.list')
exps = list(exp_details['exposure'])
nites = list(exp_details['nite'])

dir_prefix_exp = '/pnfs/des/persistent/gw/exp/'
dir_prefix_fp = '/pnfs/des/persistent/gw/forcephoto/images/dp'

stats_dict = {}
finished = 0

for exp, nite in zip(exps, nites):

    exp = str(exp)
    nite = str(nite)

    print(dir_prefix_exp + nite + '/' + exp + '/dp' + season)
    files_finished = glob.glob(dir_prefix_exp + nite + '/' + exp + '/dp' + season + '/*_*/*.tar.gz')
    files_filled = glob.glob(dir_prefix_exp + nite + '/' + exp + '/dp' + season + '/*_*/stamps*')
    files_failed = glob.glob(dir_prefix_exp + nite + '/' + exp + '/dp' + season + '/*_*/*.FAIL')
    finished_ccds = [f.split('/')[-2] for f in files_finished]
    failed_ccds = []
    fail_types = []

    for f in files_failed:
        failed_ccds.append(f.split('/')[-2])
        fail_types.append(f.split('/')[-1])

    dict_fails = []

    if len(files_finished) == 0 and len(files_failed) == 0:
        print('Nothing has finished for ' + exp + '.')
    
    elif len(files_failed) == 0 and len(files_finished) > 0:
        print(str(len(finished_ccds)) + ' ccds have finished, and none have failed.')
        #for finished_ccd in finished_ccds:
        #    print(finished_ccd + ' finished and did not fail.')

    else:
        run = []
    
        for ccd in finished_ccds:
            if ccd in failed_ccds:
                run.append(fail_types[failed_ccds.index(ccd)].split('.')[0])
                
        for run_num in np.unique(run):
            count = 0
            for r in run:
                if r == run_num:
                    count += 1
            print(run_num + ': {:0.2f}'.format(float(count)/float(len(files_finished))*100) + '% of CCDs failed on this step. (' + str(count) + ' out of ' + str(len(files_finished))+').')
            
            try:
                stats_dict[run_num] += count
            except KeyError:
                stats_dict[run_num] = count

        finished += len(files_finished)
        
    print(dir_prefix_fp + season + '/' + nite + '/' + exp + '/')
    files_fits = glob.glob(dir_prefix_fp + season + '/' + nite + '/' + exp + '/*.fits')
    files_psf = glob.glob(dir_prefix_fp + season + '/' + nite + '/' + exp + '/*.psf')
    
    all_forcephot_present = False

    if len(files_fits) > 0 and len(files_psf) > 0:
        print('ForcePhoto outputs for '+ exp + ' are present.')
        all_forcephot_present = True
    else:
        if len(files_fits) == 0 and len(files_psf) == 0:
            print('Missing all ForcePhoto outputs for ' + exp + '.')
        elif len(files_fits) == 0 and len(files_psf) > 0:
            print('Missing the fits output for ' + exp + '.')
        else:
            print('Missing the psf output for ' + exp +'.')

    stats_dict['Finished'] = finished

with open('fetchJobSubStatsDict.pkl', 'wb') as f:
    pickle.dump(stats_dict, f)
