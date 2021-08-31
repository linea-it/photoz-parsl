import parsl
from condor import config
from apps import (
    run_zphot, create_galaxy_lib,
    create_filter_set, compute_galaxy_mag
)
from utils import (
    create_dir, prepare_format_output, set_partitions
)
from numpy import loadtxt
import time
import yaml
import os
import glob
import shutil
import logging
import datetime

origin_path = os.getcwd()
lephare_sandbox = f'{origin_path}/sandbox/'
create_dir(lephare_sandbox, chdir=True, rmtree=True)

logger = logging.getLogger(__name__)
handler = logging.FileHandler('run-lephare.log')
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

start_time_full = time.time()
logger.info('LePhare pipeline')
logger.info('-> Loading configurations')

# Copy config.yml to sandbox dir
shutil.copy2('../config.yml', lephare_sandbox)

# Changing run directory to sandbox in "child jobs".
config.run_dir = os.path.join(lephare_sandbox, "runinfo")

# Settings Parsl configurations
parsl.load(config)

# Loading Lephare configurations
with open("config.yml") as _file:
    phz_config = yaml.load(_file, Loader=yaml.FullLoader)

inputs = phz_config.get('inputs', {})
output_dir = phz_config.get('output_dir', {})
settings = phz_config.get('settings', {})
test_env = phz_config.get("test_environment", {})

zphot_para = inputs.get('zphot')
lephare_dir = settings.get("lephare_bin")

# Creating LePhare dirs
for x in ['filt', 'lib_bin', 'lib_mag']:
    try:
        os.mkdir(x)
    except:
        pass

start_time = time.time()

logger.info("-> Step 1: creating SED library")
gallib = create_galaxy_lib(zphot_para, lephare_dir, lephare_sandbox)
gallib.result()

logger.info("-> Step 2: creating filter transmission files")
filterset = create_filter_set(zphot_para, lephare_dir, lephare_sandbox)
filterset.result()

logger.info("-> Step 3: theoretical magnitudes library")
galmag = compute_galaxy_mag(zphot_para, lephare_dir, lephare_sandbox)
galmag.result()

logger.info("   steps 1,2 and 3 completed: %s seconds" % (int(time.time() - start_time)))

logger.info("-> Step 4: run the photo-z code on the input catalog")
start_time = time.time()

# Getting Lephare parameters
apply_corr = settings.get('photo_corr', None)
photo_type = settings.get('photo_type')
err_type = settings.get('err_type')
bands_list = settings.get('bands')
id_col = settings.get("index")
shifts = settings.get("shifts", None)
limit_sample = test_env.get("limit_sample", None) if test_env.get("turn_on", False) else None
npartition = settings.get("partitions", 50)

# Reading zphot.para
dic = dict()
with open(zphot_para, "r") as conffile:
    for line in conffile.read().splitlines():
        dic[line.split()[0]] = "".join(line.split()[1:])

sed = dic.get('GAL_SED')
paraout = dic['PARA_OUT']
cat_fmt = str(dic['CAT_FMT'])

# Preparing LePhare output format
idxs, namephotoz = prepare_format_output(bands_list, paraout)

# Getting Input Catalog
photo_files = glob.glob(inputs.get("photometric_data"))

# Limits photometric data according to selected config. (for testing)
ninterval = 0
if limit_sample:
    nfiles, ninterval = limit_sample
    photo_files = photo_files[:nfiles]

# Creating outputs directory
create_dir(output_dir)

# Settings partitions in photometrics data
partitions_list = set_partitions(photo_files, npartition, id_col)

# Creating Lephare's runs list
counter, procs = 0, list()

for item in partitions_list:
    filename = item.get("path")
    ranges = item.get("ranges")[:ninterval] if ninterval else item.get("ranges")
    for interval in ranges:
        output_dir_file = os.path.join(
            output_dir, os.path.basename(filename).replace(".parquet", "")
        )
        create_dir(output_dir_file)
        phot_out = os.path.join(
            output_dir_file,
            f'photz-{str(counter).zfill(3)}-{os.path.basename(filename)}'
        )
        procs.append(run_zphot(counter, filename, interval, shifts, phot_out, photo_type,
            err_type, apply_corr, bands_list, sed, zphot_para, id_col, cat_fmt, idxs, namephotoz,
            lephare_dir, lephare_sandbox
        ))
        counter += 1

logger.info(f'   number of parallel jobs: {str(len(procs))}')

for proc in procs:
    proc.result()

# Returning to original path
os.chdir(origin_path)

parsl.clear()

logger.info("   step 4 completed: %s seconds" % (int(time.time() - start_time)))
logger.info("Full runtime: %s seconds" % (int(time.time() - start_time_full)))
