import configparser
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--season', type=int, help="season #, as determined by main-injector.")
parser.add_argument('--recycler_mjd', type=float, help="recycler mjd")
parser.add_argument('--propid', type=str, help="propid 20##B-####")
parser.add_argument('--exp_list', type=str)
parser.add_argument('--bands', type=str)
args = parser.parse_args()

config = configparser.RawConfigParser()
config.optionxform = str

config['general'] = {'season': args.season,
                     'propid': args.propid,
                     'triggermjd': args.recycler_mjd,
                     'ups': 'False',
                     'env_setup_file': './diffimg_setup.sh',
                     'rootdir': '/pnfs/des/persistent/gw',
                     'outdir': '/fake/outdir',
                     'indir': './',
                     'db': 'destest',
                     'schema': 'marcelle',
                     'exposures_listfile': args.exp_list,
                     'bands': args.bands,
                     'GoodSNIDs': '/this/file/does/not/exist'}

config['plots'] = {'mlscore_cut': '0.7'}

config['masterlist'] = {'blacklist': 'blacklist.txt',
                        'filename_1': 'MasterExposureList_prelim.fits',
                        'filename_2': 'MasterExposureList.fits'} 

config['checkoutputs'] = {'logfile':'checkoutputs.log',
                          'ccdfile': 'checkoutputs.csv',
                          'goodfile': 'goodchecked.list',
                          'steplist': 'steplist.txt'}

config['GWFORCE'] = {'numepochs_min':'0',
                     'ncore':'8',
                     'writeDB':'True'}

config['HOSTMATCH'] = {'version': 'v1.0.1'}

config['truthtable'] = {'filename':'fakes_truth.tab',
                        'plusname':'truthplus.tab'}

config['GWmakeDataFiles'] = {'format':'snana',
                             'numepochs_min':'0',
                             '2nite_trigger':'null'}

config['GWmakeDataFiles-real'] = {'outFile_stdout':'makeDataFiles_real.stdout',
                                  'outDir_data': 'LightCurvesReal',
                                  'combined_fits':'datafiles_combined.fits'}

config['GWmakeDataFiles-fake'] = {'outFile_stdout':'makeDataFiles_fake.stdout',
                                  'outDir_data':'LightCurvesFake',
                                  'version':'KBOMAG20ALLSKY'}

with open('postproc_' + str(args.season) + '.ini', 'w') as configfile:
    config.write(configfile)
