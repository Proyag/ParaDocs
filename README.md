# ParaDocs
Extracting parallel data with original document context from raw ParaCrawl data

This repo shares its name with https://github.com/rewicks/ParaDocs and also deals with extracting document information from [ParaCrawl](https://paracrawl.eu/), but these two repos are neither forks nor affiliated in any way. The name was just too perfect to pass up!

## Environment setup:
<!-- PPTODO -->
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
`wget https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/release9/SRC-TRG/SRC-TRG.tmx.gz -O data/released/SRC-TRG.tmx.gz`
where `SRC` and `TRG` are ISO 639-1 two-letter language codes, for example:
`wget https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/release9/en-mt/en-mt.tmx.gz -O data/released//en-mt.tmx.gz`
Or download the file from https://paracrawl.eu.
* TSV file: TSV file from official ParaCrawl releases. This contains all the parallel text that's also in the TMX files, but this format is convenient for some steps.  
`wget https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/release9/SRC-TRG/SRC-TRG.txt.gz -O data/released//SRC-TRG.tmx.gz`
where `SRC` and `TRG` are ISO 639-1 two-letter language codes, for example:
`wget https://web-language-models.s3.us-east-1.amazonaws.com/paracrawl/release9/en-mt/en-mt.txt.gz -O data/released//en-mt.tmx.gz`

## Run the document extraction pipeline
### Step 1: Extract URLs
In this step, we extract the URLs and corresponding line numbers from the TMX file. Note that each line can be associated to one or many URLs where the line was present, and each URL might be associated with many line numbers.

```bash
./extract-urls.sh SRC TRG
```
