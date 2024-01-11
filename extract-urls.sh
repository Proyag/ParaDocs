#!/bin/bash
set -Eeuo pipefail

SRC=$1
TRG=$2

N_JOBS=16

DATA_HOME=data

echo "Extracting URLs from ${SRC}-${TRG} TMX file"
python extract-urls.py ${SRC} ${TRG}

for lang in ${SRC} ${TRG}; do
    echo "Sorting URLs for ${lang}"
    LC_ALL=C sort -k1,1 --parallel=${N_JOBS} -S 75% \
        ${DATA_HOME}/extracted_urls/${SRC}-${TRG}.${lang}.urls.withdupes \
        > ${DATA_HOME}/extracted_urls/${SRC}-${TRG}.${lang}.urls.sorteddupes
    echo "Reducing URLs per line for ${lang}"
    python combine-sorteddupes.py \
        ${DATA_HOME}/extracted_urls/${SRC}-${TRG}.${lang}.urls.sorteddupes \
        ${DATA_HOME}/extracted_urls/${SRC}-${TRG}.${lang}.urls
done

rm ${DATA_HOME}/extracted_urls/${SRC}-${TRG}.*.urls.{with,sorted}dupes
