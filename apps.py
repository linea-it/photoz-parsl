from parsl import python_app

@python_app
def create_galaxy_lib(zphot_para, lephare_dir, lephare_sandbox):
    """
    LePhare step 1: creating SED library
    """
    import os
    import subprocess
    import shlex

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)

    os.environ['LEPHAREWORK'] = os.getcwd()
    os.environ['LEPHAREDIR'] = os.path.dirname(os.path.normpath(lephare_dir))

    cmd_phz = f'{lephare_dir}/sedtolib -t G -c {zphot_para} '
    subplog = open('gallib.log', 'w+')
    proc = subprocess.Popen(shlex.split(cmd_phz), stdout=subplog, stderr=subplog, universal_newlines=True)
    proc.wait()
    print(f"Executed {cmd_phz}. Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def create_filter_set(zphot_para, lephare_dir, lephare_sandbox):
    """
    LePhare step 2: creating filter transmission files
    """
    import os
    import subprocess
    import shlex

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)

    os.environ['LEPHAREWORK'] = os.getcwd()
    os.environ['LEPHAREDIR'] = os.path.dirname(os.path.normpath(lephare_dir))

    cmd_phz = f'{lephare_dir}/filter -c {zphot_para} '
    subplog = open('filterset.log', 'w+')
    proc = subprocess.Popen(shlex.split(cmd_phz), stdout=subplog, stderr=subplog, universal_newlines=True)
    proc.wait()
    print(f"Executed {cmd_phz}. Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def compute_galaxy_mag(zphot_para, lephare_dir, lephare_sandbox):
    """
    LePhare step 3: theoretical magnitudes library
    """
    import os
    import subprocess
    import shlex

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)

    os.environ['LEPHAREWORK'] = os.getcwd()
    os.environ['LEPHAREDIR'] = os.path.dirname(os.path.normpath(lephare_dir))

    cmd_phz = f'{lephare_dir}/mag_gal -t G -c {zphot_para} '
    subplog = open('maggal.log', 'w+')
    proc = subprocess.Popen(shlex.split(cmd_phz), stdout=subplog, stderr=subplog, universal_newlines=True)
    proc.wait()
    print(f"Executed {cmd_phz}. Return code = {proc.returncode}")

    os.chdir(origin_path)


@python_app
def run_zphot(key, filename, interval, shifts, zphot_output, photo_type, err_type, apply_corr,
        bands, sed, zphot, col_index, cat_fmt, idxs, namephotoz, lephare_dir, lephare_sandbox):
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
        create_dir, get_photometric_columns, format_input, create_inputs_symbolic_link
    )

    origin_path = os.getcwd()
    os.chdir(lephare_sandbox)

    # Gets the list of columns used by LePhare to filter photometric data
    columns_list = get_photometric_columns(bands, photo_type, err_type, col_index, apply_corr)
 
    # Loading in memory only the range of selected rows
    table = parq.read_table(filename, columns=columns_list)[interval[0]:interval[1]]
    tb = table.to_pandas()

    # Gets the index column to be added to the final result
    col_index_values = tb.get(col_index).to_numpy()

    # Create txt input expected by Lephare
    lephare_input = format_input(
        key, tb, bands, photo_type, err_type, col_index, apply_corr, cat_fmt
    )

    lephare_run_path = f'Lephare_run_{key}'
    create_dir(lephare_run_path)

    shutil.move(lephare_input, lephare_run_path)
    create_inputs_symbolic_link(lephare_sandbox, lephare_run_path)

    os.chdir(lephare_run_path)
    os.environ['LEPHAREWORK'] = os.getcwd()
    os.environ['LEPHAREDIR'] = os.path.dirname(os.path.normpath(lephare_dir))

    shifts = f'-APPLY_SYSSHIFT {shifts}' if shifts else str()

    phzout = 'lephare.out'

    cmd_phz = f'{lephare_dir}/zphota -c {zphot} -CAT_IN {lephare_input} -CAT_OUT {phzout} {shifts}'
    subplog = open('zphot.log', 'w+')

    proc = subprocess.Popen(shlex.split(cmd_phz), stdout=subplog, stderr=subplog, universal_newlines=True)
    proc.wait()
    print(f"Executed {cmd_phz}. Return code = {proc.returncode}")

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
