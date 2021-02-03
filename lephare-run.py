import parsl
from condor import config
from apps import run_lephare
from utils import (
    create_dir, untar_file, replace_in_file,
    prepare_format_output, set_partitions
)
from numpy import loadtxt
import time
import yaml
import os
import glob


start_time = time.time()

# Creating sandbox to Lephare execution
origin_path = os.getcwd()
lephare_sandbox = f'{origin_path}/sandbox/'
create_dir(lephare_sandbox, chdir=True, rmtree=True)

# Changing run directory to sandbox in "child jobs".
config.run_dir = os.path.join(lephare_sandbox, "runinfo")

# Settings Parsl configurations
parsl.load(config)
#parsl.load()

# Loading Lephare configurations
with open("../config.yml") as _file:
    phz_config = yaml.load(_file, Loader=yaml.FullLoader)
inputs = phz_config.get('inputs', {})
settings = phz_config.get('settings', {})
test_env = phz_config.get("test_environment", {})

# Unzipping Photoz Training files
untar_file(inputs.get('trainning_file'))

# Getting shifts
shifts = str(loadtxt("lephare_shifts.txt", dtype="str"))

# Getting Lephare parameters
apply_corr = settings.get('photo_corr', None)
photo_type = settings.get('photo_type')
err_type = settings.get('err_type')
bands_list = settings.get('bands')
id_col = settings.get("index")
limit_sample = test_env.get("limit_sample", None)
npartition = settings.get("partitions", 50)
lephare_dir = settings.get("lephare_bin")

# Preparing LePhare output format
replace_in_file("zphot.para", "AUTO_ADAPT(.*)YES", "AUTO_ADAPT NO")
idxs, namephotoz = prepare_format_output(bands_list, "zphot_output.para")

# Reading zphot.para
dic = dict()
with open("zphot.para", "r") as conffile:
    for line in conffile.read().splitlines():
        dic[line.split()[0]] = "".join(line.split()[1:])
sed = dic.get('GAL_SED')
paraout = dic['PARA_OUT']
cat_fmt = str(dic['CAT_FMT'])

# Preparing to execute LePhare in parallel (each thread working in its own file)
global_lephare_parameters, procs = list(), list()
lephare_parameters = dict()
lephare_parameters['sedFile'] =  'spectra.param'
lephare_parameters['nobj_photoz'] = 0  # being initialized
lephare_parameters['nobj_gal'] = 0  # being initialized
lephare_parameters['dimension_galaxies'] = 0  # being initialized

# Getting Input Catalog
photo_files = glob.glob(inputs.get("photometric_data"))

ninterval = 0
if limit_sample:
    nfiles, ninterval = limit_sample
    photo_files = photo_files[:nfiles]

# Creating outputs directory
output_dir = os.path.normpath(f"{lephare_sandbox}/outputs/")
create_dir(output_dir)

# Settings partitions in photometrics data
partitions_list = set_partitions(photo_files, npartition, id_col)

# Creating Lephare's runs list
counter = 0
for item in partitions_list:
    filename = item.get("path")
    ranges = item.get("ranges")[:ninterval] if ninterval else item.get("ranges")
    for interval in ranges:
        global_lephare_parameters.append(lephare_parameters)
        output_dir_file = os.path.join(
            output_dir, os.path.basename(filename).replace(".parquet", "")
        )
        create_dir(output_dir_file)
        phot_out = os.path.join(
            output_dir_file,
            f'photz-{str(counter).zfill(3)}-{os.path.basename(filename)}'
        )
        procs.append(run_lephare(counter, filename, interval, shifts, phot_out, photo_type,
            err_type, apply_corr, bands_list, sed, paraout, global_lephare_parameters,
            id_col, cat_fmt, idxs, namephotoz, lephare_dir, lephare_sandbox
        ))
        counter += 1

for proc in procs:
    proc.result()

# Returning to original path
os.chdir(origin_path)

print("--- %s seconds ---" % (time.time() - start_time))
parsl.clear()
