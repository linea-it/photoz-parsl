from parsl import python_app


@python_app
def create_galaxy_lib(
    zphot_para, lephare_dir, lephare_sandbox,
    stdout='galaxy_lib.log', level='info'
    ):
    """ LePhare step 1: creating SED library

    Args:
        zphot_para (str): zphot_para path
        lephare_dir (str): the LePhare installation directory path
        lephare_sandbox (str): working directory path
    """
    import os
    import subprocess
    import shlex
    from utils import get_logger
    
    logger = get_logger(
        name='sedtolib', level=level,
        stdout=os.path.join(lephare_sandbox, stdout)
    )

    logger.info('Creating SED library')

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)

    os.environ['LEPHAREWORK'] = os.getcwd()
    logger.info('LEPHAREWORK: {}'.format(os.environ['LEPHAREWORK']))

    cmd_phz = f'{lephare_dir}/sedtolib -t G -c {zphot_para}'
    subplog = open('sedtolib.run', 'w+')
    logger.info(f"Executing {cmd_phz}")
    proc = subprocess.Popen(
        shlex.split(cmd_phz), stdout=subplog,
        stderr=subplog, universal_newlines=True
    )
    proc.wait()
    logger.info(f"Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def create_filter_set(
    zphot_para, lephare_dir, lephare_sandbox,
    stdout='filter_set.log', level='info'
    ):
    """ LePhare step 2: creating filter transmission files

    Args:
        zphot_para (str): zphot_para path
        lephare_dir (str): the LePhare installation directory path
        lephare_sandbox (str): working directory path
    """
    import os
    import subprocess
    import shlex
    from utils import get_logger
    
    logger = get_logger(
        name='filter', level=level,
        stdout=os.path.join(lephare_sandbox, stdout)
    )

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)
    logger.info('Creating filter transmission files')

    os.environ['LEPHAREWORK'] = os.getcwd()
    logger.info('LEPHAREWORK: {}'.format(os.environ['LEPHAREWORK']))

    cmd_phz = f'{lephare_dir}/filter -c {zphot_para} '
    subplog = open('filter.run', 'w+')
    logger.info(f"Executing {cmd_phz}")
    proc = subprocess.Popen(
        shlex.split(cmd_phz), stdout=subplog,
        stderr=subplog, universal_newlines=True
    )
    proc.wait()
    logger.info(f"Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def compute_galaxy_mag(
    zphot_para, lephare_dir, lephare_sandbox,
    stdout='galaxy_mag.log', level='info'
    ):
    """ LePhare step 3: theoretical magnitudes library

    Args:
        zphot_para (str): zphot_para path
        lephare_dir (str): the LePhare installation directory path
        lephare_sandbox (str): working directory path
    """
    import os
    import subprocess
    import shlex
    from utils import get_logger
    
    logger = get_logger(
        name='mag_gal', level=level,
        stdout=os.path.join(lephare_sandbox, stdout)
    )

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)
    logger.info('Computing theoretical magnitudes')

    os.environ['LEPHAREWORK'] = os.getcwd()
    logger.info('LEPHAREWORK: {}'.format(os.environ['LEPHAREWORK']))

    cmd_phz = f'{lephare_dir}/mag_gal -t G -c {zphot_para} '
    subplog = open('mag_gal.run', 'w+')
    logger.info(f"Executing {cmd_phz}")
    proc = subprocess.Popen(
        shlex.split(cmd_phz), stdout=subplog,
        stderr=subplog, universal_newlines=True
    )
    proc.wait()
    logger.info(f"Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def run_zphot(
    key, filename, interval, shifts, zphot_output, photo_type,
    err_type, apply_corr, bands, zphot, col_index, cat_fmt,
    idxs, namephotoz, lephare_dir, lephare_sandbox,
    stdout='zphot.log', level='info'):
    """  Runs LePhare for each input data (fits) """

    import shutil
    import shlex
    import subprocess
    import pyarrow as pa
    import pyarrow.parquet as parq
    import pandas as pd
    import os
    from numpy import loadtxt
    from utils import (
        create_dir, get_photometric_columns,
        format_input, create_inputs_symbolic_link
    )
    from utils import get_logger

    lephare_run_path = os.path.join(lephare_sandbox, f'zphot-{key}')
    create_dir(lephare_run_path)

    logger = get_logger(
        name='zphot', level=level,
        stdout=os.path.join(lephare_run_path, stdout)
    )

    logger.info('Running zphot ID: {}'.format(key))
    logger.info('Input file: {}'.format(filename))
    logger.info('Interval: {}'.format(interval))

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)  

    # Gets the list of columns used by LePhare to filter photometric data
    columns_list = get_photometric_columns(
        bands, photo_type, err_type, col_index, apply_corr
    )

    # Loading in memory only the range of selected rows
    table = parq.read_table(
        filename, columns=columns_list
    )[interval[0]:interval[1]]
    tb = table.to_pandas()

    # Gets the index column to be added to the final result
    col_index_values = tb.get(col_index).to_numpy()

    # Create txt input expected by Lephare
    lephare_input = format_input(
        key, tb, bands, photo_type, err_type, col_index, apply_corr, cat_fmt
    )

    shutil.move(lephare_input, lephare_run_path)
    create_inputs_symbolic_link(lephare_sandbox, lephare_run_path)

    os.chdir(lephare_run_path)
    os.environ['LEPHAREWORK'] = os.getcwd()

    shifts = f'-APPLY_SYSSHIFT {shifts}' if shifts else str()
    phzout = 'lephare.out'

    logger.info(f'LEPHAREWORK: {os.getcwd()}')
    logger.info(f'LEPHAREDIR: {os.getenv("LEPHAREDIR")}')

    cmd_phz = f'{lephare_dir}/zphota -c {zphot} -CAT_IN {lephare_input} -CAT_OUT {phzout} {shifts}'

    logger.info(f"Run zphot cmd: {cmd_phz}")

    subplog = open('zphot.run', 'w+')

    proc = subprocess.Popen(
        shlex.split(cmd_phz), stdout=subplog,
        stderr=subplog, universal_newlines=True
    )
    proc.wait()
    logger.info(f"Return code = {proc.returncode}")

    # Loading lePhare output only with selected columns (idxs)
    zphotoz = loadtxt(phzout, comments='#', usecols=(idxs), unpack=True)

    # Calculating the photoz error as the mean of Z_BEST68_LOW and Z_BEST68_HIGH
    ihigh, ilow = namephotoz.index('Z_BEST68_HIGH'), namephotoz.index('Z_BEST68_LOW')

    photozerr = abs(zphotoz[ihigh]-zphotoz[ilow])/2. #The name of the column on file must be ERR_Z

    _parquet = {}

    for value, name in zip(zphotoz, namephotoz):
        _parquet[name.lower()] = value

    _parquet[col_index.lower()] = col_index_values
    _parquet['err_z'] = photozerr

    df = pd.DataFrame(_parquet)
    table = pa.Table.from_pandas(df, preserve_index=False)

    parq.write_table(table, zphot_output)

    os.chdir(origin_path)

    return {"name": os.path.basename(filename), "file": zphot_output}


@python_app
def join_partitions(param, stdout='join.log', level='info'):
    """_summary_

    Args:
        param (_type_): _description_
    """

    from utils import get_logger

    logger = get_logger(
        name='join_partitions', stdout=stdout, level=level
    )

    logger.info('Joining partitions')
    logger.info(f'Param: {param}')
