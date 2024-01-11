#!/bin/bash
set -Eeuo pipefail

SRC=$1
TRG=$2

if [[ ${#SRC} == 2 ]]; then
    SRC1=${SRC}
    SRC2=`./lang_codes.sh ${SRC}`
    echo "Converted ${SRC} to ${SRC2}. Make sure to use ${SRC2} in following steps."
elif [[ ${#SRC} == 3 ]]; then
    SRC1=`./lang_codes.sh ${SRC}`
    SRC2=${SRC}
else
    echo "Invalid source language code: ${SRC}. Use ISO 639-1 or ISO 639-2."
    exit 1
fi
if [[ ${#TRG} == 2 ]]; then
    TRG1=${TRG}
    TRG2=`./lang_codes.sh ${TRG}`
    echo "Converted ${TRG} to ${TRG2}. Make sure to use ${TRG2} in following steps."
elif [[ ${#TRG} == 3 ]]; then
    TRG1=`./lang_codes.sh ${TRG}`
    TRG2=${TRG}
else
    echo "Invalid source language code: ${SRC}. Use ISO 639-1 or ISO 639-2."
    exit 1
fi

echo "Downloading ${SRC2}-${TRG2} data from Paracrawl release 9"
for f in tmx txt; do
    wget https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/release9/${SRC1}-${TRG1}/${SRC1}-${TRG1}.$f.gz -O data/released/${SRC2}-${TRG2}.$f.gz
done
