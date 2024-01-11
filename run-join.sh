#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C

# Edit this!
RAWDATA_DIR=classified-fasttext

SRC=$1
TRG=$2
COLLECTION=$3
LANG=$4

if [[ ${LANG} != ${SRC} ]] && [[ ${LANG} != ${TRG} ]]; then
    echo "Usage: $0 <source> <target> <collection> <language>"
    echo "where <language> is either ${SRC} or ${TRG}"
    exit 1
fi

DATA_HOME=data
URLS_DIR=${DATA_HOME}/extracted_urls
OUTPUT_DIR=${DATA_HOME}/joined/${SRC}-${TRG}

mkdir -p ${OUTPUT_DIR}

URL_FILE=${RAWDATA_DIR}/${COLLECTION}/${LANG}/url.gz
DOC_FILE=${RAWDATA_DIR}/${COLLECTION}/${LANG}/text.gz
TMX_URLS=${URLS_DIR}/${SRC}-${TRG}.${LANG}.urls
echo "File sizes:"
{
    ls -lh ${TMX_URLS} | awk '{print $9, $5}'
    ls -lh ${URL_FILE} | awk '{print $9, $5}'
    ls -lh ${DOC_FILE} | awk '{print $9, $5}'
} | column -t

echo -ne "Starting join for ${COLLECTION} ${LANG} at "
date

# Strip http[s]:// and trailing slash from url.gz, number lines,
# sort by URL, join with the sorted URLs from the TMX, sort by line number,
# join with line numbers of text.gz, throw away line numbers,
# and compress to output file
pigz -cd ${URL_FILE} | tr [:upper:] [:lower:] | sed -E -e "s/^https?:\/\///" -e "s/\/$//" | nl -ba -p -n'rz' -w15 \
    | sort -t $'\t' -k 2,2 --parallel=16 -S 20% \
    | join -t $'\t' -1 1 -2 2 ${TMX_URLS} - \
    | sort -t $'\t' -k 3,3 --parallel=16 -S 70% \
    | join -t $'\t' -1 3 -2 1 - <(pigz -cd ${DOC_FILE} | pv -btrl | nl -ba -p -n'rz' -w 15) \
    | cut -f2- \
    | pigz -p8 \
    > ${OUTPUT_DIR}/${SRC}-${TRG}.${COLLECTION}.${LANG}.joined.gz

# Output will have columns [URL, comma-separated TMX line numbers, base64 document]

echo -n "Finished join for ${COLLECTION} ${LANG} at "
date
echo
