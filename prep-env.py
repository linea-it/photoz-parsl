import os
import shutil
import re

if not os.getenv('LEPHAREDIR'):
    print('LEPHAREDIR not set')
    exit(1)

shutil.copytree('sample-data/DES/filt/', f'{os.getenv("LEPHAREDIR")}/filt/des', dirs_exist_ok=True)

zphot_para_template = 'sample-data/zphot/zphot.para.template'
zphot_para = 'sample-data/zphot/zphot.para'
shutil.copy(zphot_para_template, zphot_para)

with open(zphot_para_template, "r") as sources:
    lines = sources.readlines()
with open(zphot_para, "w") as sources:
    for line in lines:
        if 'PARA_OUT' in line:
            sources.write(re.sub(r"^PARA_OUT", f"PARA_OUT {os.getenv('PHZ_ROOT')}/sample-data/zphot/zphot_output.para", line))
        elif 'GAL_SED' in line:
            sources.write(re.sub(r"^GAL_SED", f"GAL_SED {os.getenv('PHZ_ROOT')}/sample-data/DES/SED/COSMOS_SED/COSMOS_MOD.list", line))
        else:
            sources.write(line)

sample_template = 'sample-data/sample.yml.template'
sample_yml = 'sample-data/sample.yml'
shutil.copy(sample_template, sample_yml)

with open(sample_template, "r") as sources:
    lines = sources.readlines()
with open(sample_yml, "w") as sources:
    for line in lines:
        if 'PHZ_ROOT' in line:
            sources.write(re.sub(r"PHZ_ROOT", f"{os.getenv('PHZ_ROOT')}", line))
        elif 'LEPHAREDIR' in line:
            sources.write(re.sub(r"LEPHAREDIR", f"{os.getenv('LEPHAREDIR')}/source", line))
        else:
            sources.write(line)

