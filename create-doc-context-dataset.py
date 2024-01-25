import sys
import re
import argparse
from base64 import standard_b64decode

SEGMENT_DELIMITER = '<docline>'

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('--input', '-i', type=str, required=True, help='Path to input shard')
parser.add_argument('--output', '-o', type=str, required=True, help='Path to output file')
parser.add_argument('--sentence-file', type=str, required=True, help='Path to file with sentences')
parser.add_argument('--context-size', '-c', type=int, default=512, help='Context size (number of tokens)')
parser.add_argument('--keep-sentence', '-k', action='store_true', help='Keep the sentence at the end of the document context')
parser.add_argument('--quiet', '-q', action='store_true', help="No progress updates")
parser.add_argument('--debug-missing', type=int, default=0, help="if >0, print debug info for missing sentences. No output; quits after arg sentences")

args = parser.parse_args()

# Largest file (German) is 28G, so just read into memory to index lines directly
if not args.quiet:
    print(f"Reading sentences from {args.sentence_file} into memory", file=sys.stderr)
with open(args.sentence_file) as f:
    sentences = f.readlines()
if not args.quiet:
    print(f"Read {len(sentences)} parallel sentences into memory", file=sys.stderr)

total_url_matches = 0
text_mismatches = 0
urls_read = 0

blank_lines = re.compile(r'\n\s*\n')

with open(args.input) as f_in, open(args.output, 'w') as f_out:
    for line in f_in:
        url, linenums, doc = line.strip().split('\t')
        urls_read += 1
        if "unknown" in url and '.' not in url:
            # "unknown" URLs
            continue
        linenums = [int(x) for x in linenums.split(',')]  # linenums are 1-indexed
        doc = standard_b64decode(doc).decode('utf-8', errors='ignore')
        doc = blank_lines.sub('\n', doc).replace('\n', f" {SEGMENT_DELIMITER} ")
        # Remove extra whitespaces from doc
        doc = " ".join(doc.split())
        for linenum in linenums:
            total_url_matches += 1
            sentence = sentences[linenum - 1].rstrip()
            end = doc.find(sentence)
            if end == -1:
                # sentence not in doc even though URL matched
                text_mismatches += 1
                if args.debug_missing > 0:
                    # Debugging only, to stdout
                    print(f"Mismatch: {linenum} ||| {url} ||| {sentence} ||| {doc}")
                    if text_mismatches >= args.debug_missing:
                        sys.exit(0)
                continue
            subdoc = doc[:end].rstrip().split()
            start = max(0, len(subdoc) - args.context_size)
            context = ' '.join(subdoc[start:end])
            if args.keep_sentence:
                if context.rstrip().endswith(SEGMENT_DELIMITER):
                    context = context.rstrip() + " " + sentence
                else:
                    context = context + f" {SEGMENT_DELIMITER} " + sentence
            print(f"{linenum}\t{url}\t{sentence}\t{context}", file=f_out)
        if not args.quiet and urls_read % 10000 == 0:
            print(f"Processed {urls_read} URLs", end='\r', file=sys.stderr)

if not args.quiet:
    print(f"Processed {urls_read} URLs", file=sys.stderr)
    print(f"{text_mismatches} mismatches out of {total_url_matches} total URL matches", file=sys.stderr)
