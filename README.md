# ParaCrawl-Context
Extracting parallel data with original document context from raw [ParaCrawl](https://paracrawl.eu/) data.  
For details, see the associated ACL 2024 paper: [Document-Level Machine Translation with Large-Scale Public Parallel Corpora](https://proyag.github.io/files/papers/docmt.pdf).

## Environment setup:
* Create a conda env
```bash
conda create -n context python=3.10
conda activate context
```
* Install Python packages and command-line tools
```bash
pip install -r requirements.txt
sudo apt install parallel pigz pv
```

## Required files
For all language pairs:
* classified-fasttext: Raw URLs and base64 encoded documents from ParaCrawl crawls that the corpora were extracted from. These can be downloaded from the "Language Classified Web Text from ParaCrawl" section on [this page](https://paracrawl.eu/moredata). Not everything was released here, so our process will be lossy.

Per language pair:
* TMX file: TMX files from official ParaCrawl releases.  
* TSV file: TSV file from official ParaCrawl releases. This contains all the parallel text that's also in the TMX files, but this format is convenient for some steps.  
You can get both required files with `./download-paracrawl.sh SRC TRG`. Use three-letter ISO 639-2 language codes. If only know two-letter ISO 639-1 codes, use `./lang_codes.sh TWO_LETTER_CODE` to get the three-letter code. If links in `download-paracrawl.sh` are broken, you can download the TMX and TSV files from https://paracrawl.eu, place them in `data/released`, and *rename them to use three-letter language codes*.

## Run the document extraction pipeline
### Step 1: Extract URLs
In this step, we extract the URLs and corresponding line numbers from the TMX file. Note that each line can be associated to one or many URLs where the line was present, and each URL might be associated with many line numbers.

```bash
./extract-urls.sh SRC TRG
```

### Step 2: Run join
This step basically joins the extracted URLs with the {URL, document} pairs from the classified-fasttext data.

First, edit the `RAWDATA_DIR` variable in `run-join.sh` to point to your copy of `classified-fasttext`.
Then run `./run-join.sh SRC TRG COLLECTION LANG`, `COLLECTION` is one of the collections in `classified-fasttext` like `wide00006` or `philipp`, and `LANG` is either `SRC` or `TRG`. You can also loop over all the collections and both `LANG`s, but this step is very heavy and long-running.

For example,
```bash
./run-join.sh eng mlt wide00006 mlt
```

To run everything, run
```bash
for collection in GWB-20191109192916 hieu marta philipp wide00006 wide00015 wide00016; do
    for lang in SRC TRG; do
        ./run-join.sh SRC TRG ${collection} ${lang}
    done
done
```
but remember that **this will take a long time** to run for most language pairs.

### Step 3: Extract contexts
The main part of this process is highly parallelisable. `get-context.sh` can be run in two ways: locally or as a job array on a SLURM cluster.

Usage: `./get-contexts.sh [-n N_JOBS] [-s] [-a SLURM_ARGS...] [-c CONTEXT] [-f] SRC TRG`

Arguments are:
* `-s`: Enables SLURM mode. Run locally if not provided.
* `-n N_JOBS`: Number of parallel jobs if run locally, otherwise number of parallel jobs per SLURM array job. Default: 4.
* `-a ARGS`: SLURM job arguments. Remember to wrap in quotes. For example, `-a "-A ACCOUNT -p PARTITION --nodes 1 --time 6:00:00"`
* `-c CONTEXT`: Number of tokens per retrieved context (including special sentence delimiter tokens). Default: 512.
* `-f`: Force re-splitting joined files. Useful if splitting was interrupted.

NOTE: Each job will hold one side of the sentence-level parallel corpus in memory, so take that into account when choosing `N_JOBS`.

### Output data
The final output files can be found in `data/contexts_per_line/SRC-TRG.{SRC,TRG}.context512.per_line.gz`. These are gzipped TSV files where the columns are `line_number`, `URL`, `sentence`, `context`. You can use the line numbers to match these with the lines from the original ParaCrawl TMX/TSV file. The `context` field has up to 1000 contexts (the same line may have come from many different sources) separated by `|||` as a delimiter by default. Line breaks in the original context have been replaced by a special `<docline>` token.

## Released data
Datasets for 5 language pairs have already been released at https://huggingface.co/datasets/Proyag/paracrawl_context.
