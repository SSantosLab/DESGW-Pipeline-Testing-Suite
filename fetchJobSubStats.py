import glob
import numpy as np
import configparser

parser = argparse.ArgumentParser()
parser.add_argument('--nite', type=str)
parser.add_argument('--season', type=str)
parser.add_argument('--exp_list', type=str)
args = parser.parse_args()

exp_file = open(args.exp_list, 'r')
exp_lines = exp_file.readlines()
exps = [exp_line.strip().split()[0] for exp_line in exp_list]

dir_prefix_exp = '/pnfs/des/persistent/gw/exp/'
dir_prefix_fp = '/pnfs/des/persistent/gw/forcephoto/images/'

for exp in exps:
    print(dir_prefix_exp + nite + exp + '/' + season)
    files_finished = glob.glob(dir_prefix_exp + nite + exp + '/' + season + '*_*/*.tar.gz')
    finished_ccds = [f.split('/')[-2] for f in files_finished]
    files_filled = glob.glob(dir_prefix_exp + nite + exp + '/' + season + '*_*/stamps*')
    files_failed = glob.glob(dir_prefix_exp + nite + exp + '/' + season + '*_*/*.FAIL')
    failed_ccds = [f.split('/')[-2] for f in files_failed]
    fail_types = [f.split('/')[-1] for f in files_failed]
    if len(files_finished) == 0 and len(files_failed) == 0:
        print('Nothing has finished for ' + exp)
    run = []
    for ccd in finished_ccds:
        if ccd in failed_ccds:
            run.append(fail_types[failed_ccds.index(ccd)].split('.')[0])
    for run_num in np.unique(run):
        count = 0
        for r in run:
            if r == run_num:
                count += 1
        print(run_num + ':' + str(float(count)/float(len(files_finished))*100) + '% of CCDs failed on this step. (' + str(count) + ' out of ' + str(len(files_finished))+').')
        
    print(dir_prefix_fp + season + nite + exp + '/')
    files_fits = glob.glob(dir_prefix_fp + season + nite + exp + '/*.fits')
    files_psf = glob.glob(dir_prefix_fp + season + nite + exp + '/*.psf')

    if len(files_fits) > 0 and len(files_psf) > 0:
        print('All ForcePhoto outputs for '+ exp + ' are present.')
    else:
        if len(files_fits) == 0:
            print('Missing the fits output for ' + exp + '.')
        else:
            print('Missing the psf output for ' + exp +'.')
