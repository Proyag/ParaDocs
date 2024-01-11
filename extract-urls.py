import os
import sys
import pgzip
from lxml import etree
import logging
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def strip_url(url: str):
    url = url.rstrip('/').lower()
    if url.startswith('http:') or url.startswith('https:'):
        url = url.split('//', 1)[1]
    return url

DATA_HOME="data"

src = sys.argv[1]
trg = sys.argv[2]
tmx_file = os.path.join(DATA_HOME, "released", f"{src}-{trg}.tmx.gz")

# Read URLs from the TMX files
with pgzip.open(tmx_file, thread=8) as f_tmx, \
     open(os.path.join(DATA_HOME, "extracted_urls", f"{src}-{trg}.{src}.urls.withdupes"), 'w') as f_src, \
     open(os.path.join(DATA_HOME, "extracted_urls", f"{src}-{trg}.{trg}.urls.withdupes"), 'w') as f_trg:
    logging.info(f"Reading URLs from {tmx_file}")
    tmx_elements = etree.iterparse(f_tmx, tag="tu")
    for _, tu in tmx_elements:
        tuid = tu.attrib['tuid']
        # The first tuv is en, the second is fr
        tuv_src = tu.findall('tuv')[0]
        tuv_trg = tu.findall('tuv')[1]
        for i in tuv_src.iter('prop'):
            if i.attrib['type'] == 'source-document':
                url = strip_url(i.text)
                print(f"{url}\t{tuid}", file=f_src)
        for i in tuv_trg.iter('prop'):
            if i.attrib['type'] == 'source-document':
                url = strip_url(i.text)
                print(f"{url}\t{tuid}", file=f_trg)
        if int(tuid) % 10000 == 0:
            print(f"{tuid} TUs read", end='\r', file=sys.stderr)
        tu.getparent().remove(tu)
    logging.info(f"{tuid} TUs read.")
