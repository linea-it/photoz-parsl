from parsl import python_app


@python_app
def run_lephare(key, filename, interval, shifts, zphot_output, photo_type, err_type, apply_corr,
        bands, sed, paraout, global_lephare_parameters, col_index, cat_fmt, idxs, namephotoz, 
        lephare_dir, lephare_sandbox):
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

    columns_list = get_photometric_columns(bands, photo_type, err_type, col_index, apply_corr)
 
    # Loading in memory only the range of selected rows
    table = parq.read_table(filename, columns=columns_list)[interval[0]:interval[1]]
    tb = table.to_pandas()

    lephare_input = format_input(
        key, tb, bands, photo_type, err_type, col_index, apply_corr, cat_fmt
    )

    lephare_run_path = f'Lephare_run_{key}'
    create_dir(lephare_run_path)

    shutil.move(lephare_input, lephare_run_path)
    create_inputs_symbolic_link(lephare_sandbox, lephare_run_path)

    os.chdir(lephare_run_path)
    os.environ['LEPHAREWORK'] = os.getcwd()
    os.environ['LEPHAREDIR'] = lephare_dir

    phzout = 'lephare.out'

    cmd_phz = f'{lephare_dir}/bin/zphota -c zphot.para -PARA_OUT {paraout} -CAT_IN {lephare_input} -CAT_OUT {phzout} -APPLY_SYSSHIFT {shifts}'
    subplog = open('lephare.log', 'w+')

    proc = subprocess.Popen(shlex.split(cmd_phz), stdout=subplog, stderr=subplog, universal_newlines=True)
    proc.wait()
    print(f"Executed {cmd_phz}. Return code = {proc.returncode}")

    cmd_sed = f'cp {sed} {global_lephare_parameters[key]["sedFile"]}'
    res = subprocess.run(shlex.split(cmd_sed))
    print(f"Executed {cmd_sed}. Return code = {res.returncode}")

    # Loading lePhare output only with selected columns (idxs)
    zphotoz = loadtxt(phzout, comments='#', usecols=(idxs), unpack=True)

    # Calculating the photoz error as the mean of Z_BEST68_LOW and Z_BEST68_HIGH
    ihigh, ilow = namephotoz.index('Z_BEST68_HIGH'), namephotoz.index('Z_BEST68_LOW')

    photozerr = abs(zphotoz[ihigh]-zphotoz[ilow])/2. #The name of the column on file must be ERR_Z

    _parquet = {}

    for value, name in zip(zphotoz, namephotoz):
        _parquet[name.lower()] = value

    _parquet[col_index.lower()] = tb.get(col_index).to_numpy()
    _parquet['err_z'] = photozerr

    df = pd.DataFrame(_parquet)
    table = pa.Table.from_pandas(df, preserve_index=False)

    parq.write_table(table, zphot_output)

    os.chdir(origin_path)

    return {"name": os.path.basename(filename), "file": zphot_output}