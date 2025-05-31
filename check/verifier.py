# specific to word count, to check validity of reducer files from mapreduce implementation

# read all files (keep as generator)
# go one by one, ensure none of them equal to each other
# pick the file with the smallest element by key, make that file go next line
# continue till all reach eof

from dotenv import load_dotenv
from os import getenv, listdir
from os.path import isfile, join 

def parse(line):
    word, count = line.split(" ")
    return word, int(count) 

load_dotenv('../.env')
reducer_path = getenv("REDUCER_PATH")
reducer_files = []

for i, f in enumerate(listdir(reducer_path)):
    nf = join(reducer_path, f)
    if not isfile(nf):
        continue 
    reducer_files.append(map(parse, open(nf, 'r')))
    print(f"Reducer file {i + 1}: {nf}")

while any()