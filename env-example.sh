#!/bin/bash

sed "s|PARA_OUT|PARA_OUT $PHZ_ROOT/sample-data/zphot/zphot_output.para|g" sample-data/zphot/zphot.para.template > sample-data/zphot/zphot.para
sed -i "s|GAL_SED|GAL_SED $PHZ_ROOT/sample-data/DES/SED/COSMOS_SED/COSMOS_MOD.list|g" sample-data/zphot/zphot.para

sed "s|PHZ_ROOT|$PHZ_ROOT|g" sample-data/sample.yml.template > sample-data/sample.yml
sed -i "s|LEPHAREDIR|$LEPHAREDIR/source|g" sample-data/sample.yml

mkdir $LEPHAREDIR/filt/des
cp -r sample-data/DES/filt/* $LEPHAREDIR/filt/des/