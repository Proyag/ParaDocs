import sys
import random

DELIMITER = " ||| "
CONTEXT_POOL_SIZE=10000
MAX_CONTEXTS = 1000

lines_in = 0
lines_out = 0
invalid_lines = 0

prev_line = None
prev_sentence = None
contexts = set()
urls = set()
enough_contexts = False

for line in sys.stdin:
    lines_in += 1
    try:
        line_num, url, sentence, context = line.rstrip().split('\t', 3)
    except ValueError:
        invalid_lines += 1
        continue
    if line_num == prev_line:
        if enough_contexts:
            # Don't need to collect more if we already have CONTEXT_POOL_SIZE contexts
            continue
        contexts.add(context)
        urls.add(url)
        if len(contexts) >= CONTEXT_POOL_SIZE:
            enough_contexts = True
    else:
        if prev_line is not None:
            if len(contexts) >= MAX_CONTEXTS:
                # print(f"Sampled {MAX_CONTEXTS} contexts from {len(contexts)} contexts", file=sys.stderr)
                contexts = random.sample(list(contexts), MAX_CONTEXTS)
            print(f"{prev_line}\t{DELIMITER.join(urls)}\t{prev_sentence}\t{DELIMITER.join(contexts)}")
        prev_line = line_num
        prev_sentence = sentence
        contexts = {context}
        urls = {url}
        lines_out += 1
    if lines_in % 100000 == 0:
        print(f"Lines in: {lines_in}; lines out: {lines_out}", end='\r', file=sys.stderr)


# Last one
if len(contexts) > MAX_CONTEXTS:
    # print(f"Sampled {MAX_CONTEXTS} contexts from {len(contexts)} contexts", file=sys.stderr)
    contexts = random.sample(list(contexts), MAX_CONTEXTS)
print(f"{prev_line}\t{DELIMITER.join(urls)}\t{prev_sentence}\t{DELIMITER.join(contexts)}")
lines_out += 1
print(f"Lines in: {lines_in}; lines out: {lines_out}; invalid lines: {invalid_lines}", file=sys.stderr)
