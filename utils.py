import os
import tarfile
import re
import pyarrow.parquet as pq
import shutil


def create_dir(path, chdir=False, rmtree=False):
    """ Create directory 

    Args:
        path (string): directory path
        chdir (boolean, optional): modify execution directory to what was created. Defaults to False.
        rmtree (boolean, optional): remove the directory before creating. Defaults to False.
    """

    if rmtree and os.path.isdir(path):
        shutil.rmtree(path)

    os.makedirs(path, exist_ok=True)

    if chdir:
        os.chdir(path)


def untar_file(filepath):
    """ Unzips tar files

    Args:
        filepath (string): path to tar file
    """

    tar = tarfile.open(filepath, "r")
    tar.extractall()
    tar.close()


def replace_in_file(filepath, old, new):
    """ Replaces string in text file

    Args:
        filepath (string): file path
        old (string): old string
        new (string): new string
    """

    with open(filepath, "rt") as fin:
        data = fin.read()
        data = re.sub(r"%s" % old, new, data)

    with open(filepath, "wt") as fin:
        fin.write(data)


def prepare_format_output(bands_list, zphot_output):
    """ Prepare the LePhare format output

    Args:
        bands_list (list): band list
        zphot_output (string):  zphot_output.para path

    Returns:
        tuple(list, list):
            0: index list 
            1: column name list
    """

    with open(zphot_output, 'r') as cols_out_file:
        column_names = list()

        for line in cols_out_file:
            if not line.startswith('#'):
                col = line.replace('\n', '')
                if len(col) > 0:
                    if '()' in col:
                        for band in bands_list:
                            column_names.append(col.replace('()', str('_' + band.upper())))
                    else:
                        column_names.append(col)

    out_cols = [
        ('IDENT', 'K'), ('Z_BEST', 'D'), ('Z_BEST68_LOW', 'D'),
        ('Z_BEST68_HIGH', 'D'), ('Z_ML', 'D'), ('PDZ_BEST', 'D')
    ]

    idxs, namephotoz = list(), list()

    for idx in out_cols:
        try:
            idxs.append(column_names.index(idx[0]))
        except Exception as err:
            print(f"{id[0]} column not found in zphot_output.para \n{err}")
            raise
        namephotoz.append(idx[0])

    return (idxs, namephotoz)


def set_partitions(photo_files, num_chunks, idx):
    """ Sets the partitions of each photometric file

    Args:
        photo_files (string): photometric file list
        num_chunks (integer): number of partitions 
        idx (string): index column name

    Returns:
        list: partitions list
    """

    run_list, min_size = list(), 200

    for _file in photo_files:
        chunk_list = list()
        dic_item = {'path': _file, 'ranges': chunk_list}

        pf = pq.read_table(_file, columns=[idx])
        num_entries = pf.num_rows

        if min_size:
            if num_entries/num_chunks < min_size:
                num_chunks = num_entries/min_size

        if num_chunks > num_entries or num_chunks == 0:
            num_chunks = 1

        chunk_size = int(num_entries/num_chunks)
        rest = num_entries%num_chunks

        first = 0
        last = chunk_size

        for x in range(num_chunks):
            if x < rest:
                last += 1

            chunk_list.append((first, last))
            first = last
            last += chunk_size

        run_list.append(dic_item)

    return run_list


def get_photometric_columns(bands, photo_type, err_type, idx, corr=None):
    """ Returns the photometric columns selected by Photoz Trainning

    Args:
        bands (list): Band list
        photo_type (string): string containing magnitude with {} to concatenate the band.
        err_type (string): string containing magnitude erro with {} to concatenate the band.
        idx (string): index column name
        corr (string, optional): column name to calculate the correction. Defaults to None.

    Returns:
        list: selected photometric columns
    """

    columns_list = [idx]

    if corr:
        columns_list.append(corr)

    for band in bands:
        nmag = photo_type.format(band).lower()
        nerr = err_type.format(band).lower()
        columns_list.append(nmag)
        columns_list.append(nerr)

    return columns_list


def format_input(idx, table, bands, photo_type, err_type, index_column, corr, cat_fmt="MEME"):
    """ Responsible for formatting the Lephare input

    Args:
        idx (string): thread id
        table (): [description]
        bands (list): bands list
        photo_type (string): string containing magnitude with {} to concatenate the band.
        err_type (string): string containing magnitude erro with {} to concatenate the band.
        index_column (string): index column name
        corr (string): column name to calculate the correction
        cat_fmt (str, optional): catalog format. Defaults to "MEME".

    Raises:
        BaseException: failed to find a correction value
        BaseException: unexpected catalog format

    Returns:
        string: input name created
    """

    import numpy as np

    # magnitudes that requires correction
    # TODO: has to be updated in case of dataset with more bands (deep)
    CORR_SFD98 = {
        "G": 3.185, "R": 2.140, "I": 1.571, "Z": 1.198, "Y": 1.052
    } # SFD98 20th June 2017

    ids = table.get(index_column).to_numpy()
    n_gals = len(ids)
    gal_number = range(1, n_gals + 1, 1)

    _format = ['%10d']

    mags, errs = {}, {}

    for band in bands:
        band_upper = band.upper()
        mag_values = table.get(photo_type.format(band_upper).lower()).to_numpy()

        if err_type: #TODO: in simulation case: the value will be None
            err_values = table.get(err_type.format(band_upper).lower()).to_numpy()
        else:
            err_values = np.ones_like(mag_values)

        # Eliminating 99's from sample
        mag_values[(mag_values < 0.) + (mag_values > 30.)] = -99.
        err_values[(mag_values < 0.) + (mag_values > 30.)] = -99.
        mag_values[np.isnan(mag_values)] = -99.
        err_values[np.isnan(err_values)] = -99.

        if corr:
            if not band_upper in CORR_SFD98.keys():
                mag = photo_type.format(band_upper)
                print(f"\n\nFailed to correct column magnitude {mag}")
                raise BaseException

            corr_col = table.get(corr).to_numpy()
            mag_values[mag_values != -99.] = mag_values[mag_values != -99.] - (CORR_SFD98.get(band_upper) * corr_col[mag_values != -99.])

        mags[band] = mag_values
        errs[band] = err_values
        _format.append('%.5f')
        _format.append('%.5f')

    # Calculating the context
    acont = list()

    for obj in range(n_gals):
        m, context = -1, 0
        for band in bands:
            m = m + 1

            if mags[band][obj] != -99.:
                context = context + (2**m)

        acont.append(context)

    acont = np.array(acont)
    _format.append('%.6d')

    # "True" redshifts
    z_true = np.ones(n_gals)*-99.
    _format.append('%.5f')

    columns = np.c_[gal_number]

    if cat_fmt == "MEME":
        for band in bands:
            columns = np.c_[columns, mags[band], errs[band]]
    elif cat_fmt == "MMEE":
        for band in bands:
            columns = np.c_[columns, mags[band]]
        for band in bands:
            columns = np.c_[columns, errs[band]]
    else:
        print(f"CAT_FMT: unexpected format - {cat_fmt}")
        raise BaseException

    _format.append('%10d')

    columns = np.c_[columns, acont, z_true, ids]

    input_file = f'lephare_{str(idx)}.input'
    np.savetxt(input_file, columns, fmt=_format)

    return input_file


def create_inputs_symbolic_link(sandbox, thread_dir):
    """ Creates symbolic links to all Pz Trainning inputs

    Args:
        sandbox ([type]): sandbox path
        thread_dir ([type]): thread directory path

    """

    inputs = [
        ('filt', True), ('lib_bin', True), ('lib_mag', True)
    ]

    for item in inputs:
        os.symlink(
            os.path.join(sandbox, item[0]),
            os.path.join(sandbox, thread_dir, item[0]),
            target_is_directory=item[1]
        )
