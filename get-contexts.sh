#!/bin/bash
set -Eeuo pipefail

function usage() {
	echo "Usage: $0 [-n N_JOBS] [-s] [-a SLURM_ARGS...] [-c CONTEXT] [-f] SRC TRG"
}

N_JOBS=4
SLURM=0  # 1 means run extractions as SLURM array
SLURM_ARGS=""
N_CONTEXT=512
FORCE_RESHARD=0
while getopts "n:sa:c:fh" o; do
	case "${o}" in
		h)
			usage
			exit 0
			;;
		n)
			N_JOBS=${OPTARG}
			;;
		s)
			SLURM=1
			;;
		a)
			SLURM_ARGS=${OPTARG}
			;;
		c)
			N_CONTEXT=${OPTARG}
			;;
		f)
			FORCE_RESHARD=1
			;;
		*)
			usage
			;;
	esac
done
shift $((OPTIND-1))

SRC=$1
TRG=$2

DATA_HOME=data
JOINED=${DATA_HOME}/joined/${SRC}-${TRG}

if [[ ! -d ${JOINED}/shards ]] || [[ ${FORCE_RESHARD} -eq 1 ]]; then
	echo "Sharding joined data for ${SRC}-${TRG}"
	mkdir -p ${JOINED}/shards ${DATA_HOME}/contexts_per_line
	# PPTODO
	ls -1 ${JOINED}/${SRC}-${TRG}.{GWB*,wide00006}.*.joined.gz | parallel -j ${N_JOBS} 'pigz -cd {} | split -C 30G -d -a4 - {//}/shards/{/}.shard && echo {} done'
fi

echo "Launching context extraction for ${SRC}-${TRG}"
# Split TSV file into source and target text
if [[ ! -f ${DATA_HOME}/released/${SRC}-${TRG}.${SRC} ]]; then
	pigz -cd ${DATA_HOME}/released/${SRC}-${TRG}.txt.gz | cut -f1 > ${DATA_HOME}/released/${SRC}-${TRG}.${SRC}
fi
if [[ ! -f ${DATA_HOME}/released/${SRC}-${TRG}.${TRG} ]]; then
	pigz -cd ${DATA_HOME}/released/${SRC}-${TRG}.txt.gz | cut -f2 > ${DATA_HOME}/released/${SRC}-${TRG}.${TRG}
fi

if [[ ${SLURM} -eq 0 ]]; then
	# Run local version
	for lang in ${SRC} ${TRG}; do
			echo "Extracting ${lang} contexts"
			ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard???? \
				| parallel -j${N_JOBS} 'python create-doc-context-dataset.py -q -c ${N_CONTEXT} -i {} -o {}.context${N_CONTEXT} --sentence-file' ${DATA_HOME}/released/${SRC}-${TRG}.${lang}' && echo {} done'

			echo "Sorting ${lang} contexts"
			ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard????.context${N_CONTEXT} \
				| parallel -j${N_JOBS} "sort -t$'\t' -k1,1n --parallel=8 -S 20% {} > {}.sorted && mv {}.sorted {} && echo {} done"

		echo "Combining ${lang} contexts"
		sort -m -t$'\t' -k1,1n --parallel=${N_JOBS} -S 80% ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard*.context${N_CONTEXT} \
			> ${JOINED}/${SRC}-${TRG}.${lang}.context${N_CONTEXT}
		python combine-contexts-per-line.py \
			< ${JOINED}/${SRC}-${TRG}.${lang}.context${N_CONTEXT} \
			| pigz -p4 > ${DATA_HOME}/contexts_per_line/${SRC}-${TRG}.${lang}.context${N_CONTEXT}.per_line.gz
	done
else
	# SLURM version
	mkdir -p ${JOINED}/slurm_logs
	for lang in ${SRC} ${TRG}; do
		N_SHARDS=$(ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard???? | wc -l)
		ARRAY_SIZE=`python3 -c "import math; print(math.ceil(${N_SHARDS} / ${N_JOBS}))"`

		echo "Submitting ${ARRAY_SIZE} ${lang} context extraction jobs"

		# Create script to submit to SLURM
		tmpfile=$(mktemp /tmp/context.XXXXXXX)
		cat <<-EOF > ${tmpfile}
		#!/bin/bash

		ml Anaconda3 parallel

		ALL_SHARDS=($(ls -1 ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard???? | sort))
		start=\`echo \${SLURM_ARRAY_TASK_ID} '*' ${N_JOBS} | bc\`

		echo \${ALL_SHARDS[@]:\${start}:${N_JOBS}} | tr ' ' '\n' \\
		| parallel -v -j ${N_JOBS} \\
		"python3 create-doc-context-dataset.py \\
			-q -c ${N_CONTEXT} \\
			-i {} -o {}.context${N_CONTEXT} \\
			--sentence-file ${DATA_HOME}/released/${SRC}-${TRG}.${lang} \\
		&& echo {} extraction done \\
		&& sort -t$'\t' -k1,1n --parallel=8 -S 10% {}.context${N_CONTEXT} > {}.context${N_CONTEXT}.sorted \\
		&& mv {}.context${N_CONTEXT}.sorted {}.context${N_CONTEXT} \\
		&& echo {}.context${N_CONTEXT} sorted"

		EOF

		CONTEXT_JOBID=$(
		    sbatch --parsable ${SLURM_ARGS} -J context -a 0-$(( ARRAY_SIZE - 1 )) \
		    -o ${JOINED}/slurm_logs/${SRC}-${TRG}.${lang}.context${N_CONTEXT}.%A-%a.out \
		    ${tmpfile}
		)
		echo "Submitted context extraction jobs ${CONTEXT_JOBID}"
		rm ${tmpfile}

		echo "Submitting ${lang} combine job"

		# Create script to submit to SLURM
		tmpfile=$(mktemp /tmp/context.XXXXXXX)
		cat <<-EOF > ${tmpfile}
		#!/bin/bash

		ml Anaconda3

		sort -m -t$'\t' -k1,1n --parallel=${N_JOBS} -S 80% ${JOINED}/shards/${SRC}-${TRG}.*.${lang}.joined.gz.shard*.context${N_CONTEXT} \
			> ${JOINED}/${SRC}-${TRG}.${lang}.context${N_CONTEXT} \
			&& echo ${SRC}-${TRG}.${lang}.context${N_CONTEXT} done; \
		python3 combine-contexts-per-line.py \
			< ${JOINED}/${SRC}-${TRG}.${lang}.context${N_CONTEXT} \
			| pigz -p4 > ${DATA_HOME}/contexts_per_line/${SRC}-${TRG}.${lang}.context${N_CONTEXT}.per_line.gz

		EOF

		COMBINE_JOBID=$(
		    sbatch --parsable ${SLURM_ARGS} -J combine -d afterok:${CONTEXT_JOBID} \
		    -o ${JOINED}/slurm_logs/${SRC}-${TRG}.${lang}.context${N_CONTEXT}.combine.%A.out \
		    ${tmpfile}
		)
		echo "Submitted combine job ${COMBINE_JOBID}"
		rm ${tmpfile}
	done
fi

if [[ ${SLURM} -eq 1 ]]; then
	echo
	echo "Suggested clean-up commands AFTER the jobs have finished successfully:"
	echo "rm ${DATA_HOME}/released/${SRC}-${TRG}.{${SRC},${TRG}}"
	echo "rm -r ${JOINED}"
else
	echo "Cleaning up intermediate files for ${SRC}-${TRG}"
	rm ${DATA_HOME}/released/${SRC}-${TRG}.{${SRC},${TRG}}
	rm -r ${JOINED}
fi
