import parsl
from parsl_config import get_config
from apps import (
    run_zphot, create_galaxy_lib,
    create_filter_set, compute_galaxy_mag,
    join_partitions
)
from utils import (
    create_dir, prepare_format_output, set_partitions,
    get_logger
)
import time
import yaml
import os
import glob
import logging
import argparse

LEVEL = os.environ.get('PZ_LOG_LEVEL', 'info')


def run(phz_config, parsl_config):
    """ Run Photo-z Compute 

    Args:
        phz_config (dict): Photo-z pipeline configuration - available in the config.yml
        parsl_config (dict): Parsl config
    """
    lephare_sandbox = os.getcwd()

    logger = get_logger(
        name=__name__, level=LEVEL,
        stdout=os.path.join(lephare_sandbox, 'pipeline.log')
    )

    handler_cons = logging.StreamHandler()
    logger.addHandler(handler_cons)

    start_time_full = time.time()
    logger.info('LePhare pipeline')
    logger.info('-> Loading configurations')

    # Changing run directory to sandbox in "child jobs".
    parsl_config.run_dir = os.path.join(lephare_sandbox, "runinfo")

    # Settings Parsl configurations
    parsl.clear()
    parsl.load(parsl_config)

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
    gallib = create_galaxy_lib(
        zphot_para, lephare_dir, lephare_sandbox,
        stdout='sedtolib.log', level=LEVEL
    )
    gallib.result()

    logger.info("-> Step 2: creating filter transmission files")
    filterset = create_filter_set(
        zphot_para, lephare_dir, lephare_sandbox,
        stdout='filter.log', level=LEVEL
    )
    filterset.result()

    logger.info("-> Step 3: theoretical magnitudes library")
    galmag = compute_galaxy_mag(
        zphot_para, lephare_dir, lephare_sandbox,
        stdout='mag_gal.log', level=LEVEL
    )
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
    npartition = int(settings.get("partitions", 50))

    # Reading zphot.para
    dic = dict()
    with open(zphot_para, "r") as conffile:
        for line in conffile.read().splitlines():
            dic[line.split()[0]] = "".join(line.split()[1:])

    paraout = dic.get('PARA_OUT')
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
    counter, procs = 1, list()

    for item in partitions_list:
        filename = item.get("path")
        ranges = item.get("ranges")[:ninterval] if ninterval else item.get("ranges")
        for interval in ranges:
            output_dir_file = os.path.join(
                lephare_sandbox, output_dir, os.path.basename(filename).replace(".parquet", "")
            )
            create_dir(output_dir_file)
            phot_out = os.path.join(
                output_dir_file,
                f'photz-{str(counter).zfill(5)}.parquet'
            )
            procs.append(run_zphot(counter, filename, interval, shifts, phot_out, photo_type,
                err_type, apply_corr, bands_list, zphot_para, id_col, cat_fmt, idxs, namephotoz,
                lephare_dir, lephare_sandbox, stdout=f'zphot-{counter}.log', level=LEVEL
            ))
            counter += 1

    logger.info(f'   number of parallel jobs: {str(len(procs))}')

    for proc in procs:
        proc.result()

    logger.info("   step 4 completed: %s seconds" % (int(time.time() - start_time)))
    # logger.info("-> Step 5: join the results per partition")
    # start_time = time.time()

    # procs = list()

    # # Joining the results per partition
    # for item in glob.glob(os.path.join(output_dir, "*")):
    #     if not os.path.isdir(item):
    #         logger.warn(f"   dir not found: {item}")
    #         continue

    #     basename = os.path.basename(item)
    #     procs.append(join_partitions(
    #         item, stdout=os.path.join(lephare_sandbox, f'join-{basename}.log'), level=LEVEL
    #     ))

    # for proc in procs: 
    #     proc.result()

    # logger.info("   step 5 completed: %s seconds" % (int(time.time() - start_time)))
    logger.info("Full runtime: %s seconds" % (int(time.time() - start_time_full)))
    parsl.clear()


if __name__ == '__main__':
    working_dir = os.getcwd()

    # Create the parser and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='config_path', help="yaml config path")
    parser.add_argument("-w", "--working_dir", dest="working_dir", default=working_dir, help="run directory")

    args = parser.parse_args()
    working_dir = args.working_dir
    config_path = args.config_path

    # Loading Lephare configurations
    with open(config_path) as _file:
        phz_config = yaml.load(_file, Loader=yaml.FullLoader)

    # Create sandbox dir
    lephare_sandbox = f'{working_dir}/sandbox/'
    create_dir(lephare_sandbox, chdir=True, rmtree=True)

    parsl_config = get_config(phz_config)

    # Run Photo-z
    run(phz_config, parsl_config)