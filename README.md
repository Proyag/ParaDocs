# ParaDocs
Extracting parallel data with original document context from raw ParaCrawl data

This repo shares its name with https://github.com/rewicks/ParaDocs and also deals with extracting document information from [ParaCrawl](https://paracrawl.eu/), but these two repos are neither forks nor affiliated in any way. The name was just too perfect to pass up!

## Environment setup:
* Create a conda env
```bash
conda create -n paradocs python=3.10
pip install -r requirements.txt
```
* Command-line tools
```bash
sudo apt install parallel pigz
```

## Required files
For all language pairs:
* classified-fasttext: Raw URLs and base64 encoded documents from ParaCrawl crawls that the corpora were extracted from. Not everything was released here, so our process will be lossy.

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

First, *edit the `RAWDATA_DIR` variable in `run-join.sh` to point to your copy of `classified-fasttext`.
Then run `./run-join.sh SRC TRG COLLECTION LANG`, `COLLECTION` is one of the collections in `classified-fasttext` like `wide00006` or `philipp`, and `LANG` is either `SRC` or `TRG`. You can also loop over all the collections and both `LANG`s, but this step is very heavy and long-running.

For example,
```bash
./run-join.sh eng mlt wide00006 mt
```
