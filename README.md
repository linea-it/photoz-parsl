# Photoz Parsl

Repository to test the refactoring of the photo-z related pipelines from the [DES Science Portal](https://des-portal.linea.gov.br/) using [Parsl](https://parsl.readthedocs.io/en/stable/).

## LePhare workflow
The workflow was developed aiming at the best performance based on the current resources of the LIneA environment, however it can be used in any environment that fulfills the following requirements:

* [Conda installed](https://docs.conda.io/en/latest/miniconda.html) and creation of the environment with the file environment.yml.
* [LePhare installed](https://www.cfht.hawaii.edu/~arnouts/LEPHARE/lephare.html)
* For use with [HTCondor](https://htcondor.readthedocs.io/en/latest/):
    * the workstation must be a submission machine for HTCondor.
    * the workstation must share the following resources with all HTCondor nodes:
        * the file system / directory containing the photoz-parsl repository
        * and the input data

### Instalation

1. Clone the repository and create an environment with Conda:
    ```bash
    git clone https://github.com/linea-it/photoz-parsl && cd photoz-parsl 
    conda env create -f environment.yml
    ```

2. Copy the file that sets the environment
    ```bash
    cp env.sh.template env.sh
    ```

3. Edit env.sh, adding the path to Conda (CONDAPATH) and the path to this repository (PHZ_ROOT): 
    ```bash
    export CONDAPATH=<conda path> #e.g.:/home/fulano/miniconda3/bin
    export PHZ_ROOT=<photoz-parsl repository path>
    ```

4. Sets the environment:

    ```bash
    source env.sh
    ```

5. Copy the workflow configuration file:
    ```bash
    cp config.yml.template config.yml
    ```

6. Edit config.yml with information about the inputs data and settings:

    <tr>
    <td>

    ```yml
    phz_root_dir: <repository path>
    executor: local # determines the code execution location, we currently have two options: "local" and "htcondor"
    inputs:
        photometric_data: <photometric data path>
        zphot: <zphot.para path>
    settings:
        photo_corr: <column name to magnitude correction> # e.g.: ebv
        photo_type: <magnitude column> # e.g.: SOF_BDF_MAG_{}_CORRECTED
        err_type: <magnitude error column> # e.g.: SOF_BDF_MAG_ERR_{}
        bands: <band list> # e.g.: [g,r,i,z]
        partitions: <partition numbers>
        index: <index column> # e.g.: coadd_objects_id
        lephare_bin: <lephare bin> # e.g.: $LEPHAREDIR/source
    test_environment:
        turn_on: True
        limit_sample: [1,3] # determines how many files and how many partitions the code will use. e.g.: [1,3] 1 file and 3 partitions
    ```
    </td>
    </tr>

7. Help to run the pipeline:
    ```bash
$ python lephare-run.py -h
usage: lephare-run.py [-h] [-w WORKING_DIR] config_path

positional arguments:
  config_path           yaml config path

optional arguments:
  -h, --help            show this help message and exit
  -w WORKING_DIR, --working_dir WORKING_DIR
                        run directory
``` 

### Monitoring

Parsl includes a flexible monitoring system to capture program and task state as well as resource usage over time. 

To activate the monitoring system:

1. Copy the file that active the monitoring
    ```bash
    cp monitoring.sh.template monitoring.sh
    ```

2. Edit monitoring.sh, adding the path to Conda (CONDAPATH) and the path to this repository (PHZ_ROOT): 
    ```bash
    export CONDAPATH=<conda path> #e.g.:/home/fulano/miniconda3/bin
    export PHZ_ROOT=<photoz-parsl repository path>
    ```

3. And run:

    ```bash
    source monitoring.sh
    ```

4. To view the system:
    http://localhost:55555/

    **Note:** *if your workstation is remote, you will need to make an ssh tunnel by mapping port 55555 from the remote server to your local machine.*

## License
[MIT](LICENSE.md)
