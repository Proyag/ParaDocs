#!/bin/bash
set -Eeuo pipefail

SRC=$1
TRG=$2

N_JOBS=4
SLURM=0  # 1 means run extractions as SLURM array

DATA_HOME=data
JOINED=${DATA_HOME}/joined/${SRC}-${TRG}

# echo "Sharding joined data for ${SRC}-${TRG}"
mkdir -p ${DATA_HOME}/joined/${SRC}-${TRG}/shards ${DATA_HOME}/contexts_per_line
ls -1 ${DATA_HOME}/joined/${SRC}-${TRG}/${SRC}-${TRG}.*.joined.gz | parallel -j ${N_JOBS} 'pigz -cd {} | split -C 30G -d -a4 - {//}/shards/{/}.shard && echo {} done'

echo "Launching context extraction for ${SRC}-${TRG}"
# Split TSV file into source and target text
if [[ ! -f ${DATA_HOME}/released/${SRC}-${TRG}.${SRC} ]]; then
    pigz -cd ${DATA_HOME}/released/${SRC}-${TRG}.txt.gz | cut -f1 > ${DATA_HOME}/released/${SRC}-${TRG}.${SRC}
fi
if [[ ! -f ${DATA_HOME}/released/${SRC}-${TRG}.${TRG} ]]; then
    pigz -cd ${DATA_HOME}/released/${SRC}-${TRG}.txt.gz | cut -f2 > ${DATA_HOME}/released/${SRC}-${TRG}.${TRG}
fi

for lang in ${SRC} ${TRG}; do
    if [[ ${SLURM} -eq 0 ]]; then
        echo "Extracting ${lang} contexts"
        ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard???? \
            | parallel -j${N_JOBS} 'python create-doc-context-dataset.py -q -c 512 -i {} -o {}.context512 --sentence-file' ${DATA_HOME}/released/${SRC}-${TRG}.${lang}' && echo {} done'

        echo "Sorting ${lang} contexts"
        ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard????.context512 \
            | parallel -j${N_JOBS} "sort -t$'\t' -k1,1n --parallel=8 -S 20% {} > {}.sorted && mv {}.sorted {} && echo {} done"
    else
        echo "NOT IMPLEMENTED YET"
        exit 1
    fi

    echo "Combining ${lang} contexts"
    sort -m -t$'\t' -k1,1n --parallel=${N_JOBS} -S 80% ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard*.context512 \
        > ${JOINED}/${SRC}-${TRG}.${lang}.context512
    python combine-contexts-per-line.py \
        < ${JOINED}/${SRC}-${TRG}.${lang}.context512 \
        | pigz -p4 > ${DATA_HOME}/contexts_per_line/${SRC}-${TRG}.${lang}.context512.per_line.gz
done

echo "Cleaning up intermediate files for ${SRC}-${TRG}"
rm ${DATA_HOME}/released/${SRC}-${TRG}.{${SRC},${TRG}}
rm -r ${JOINED}
