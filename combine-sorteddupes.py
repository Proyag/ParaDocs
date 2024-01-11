import sys

INPUT_FILE = sys.argv[1]
OUTPUT_FILE = sys.argv[2]

lines_in = 0
lines_out = 0
invalid_lines = 0

prev_url = None
line_nums = None
with open(INPUT_FILE) as f_in, \
     open(OUTPUT_FILE, 'w') as f_out:
    for line in f_in:
        lines_in += 1
        try:
            url, idx = line.rstrip().split('\t', 1)
        except ValueError:
            invalid_lines += 1
            continue
        if url.strip() == "":
            continue
        if url == prev_url:
            line_nums = line_nums + ',' + idx
        else:
            if prev_url is not None:
                print(f"{prev_url}\t{line_nums}", file=f_out)
            prev_url = url
            line_nums = idx
            lines_out += 1
        if lines_in % 100000 == 0:
            print(f"Lines in: {lines_in}; lines out: {lines_out}", end='\r', file=sys.stderr)

    # Last one
    print(f"{prev_url}\t{line_nums}", file=f_out)
    lines_out += 1
    print(f"Lines in: {lines_in}; lines out: {lines_out}; invalid lines: {invalid_lines}", file=sys.stderr)
