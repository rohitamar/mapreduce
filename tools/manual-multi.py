import argparse
import os
import re
import time
from collections import Counter
from multiprocessing import Pool

def process_file_chunk(file_paths):
    local_counts = Counter()
    for path in file_paths:
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    local_counts.update(re.findall(r"\b\w+\b", line.lower()))
        except Exception as e:
            print(f"Error reading {path}: {e}")
    return local_counts

def main(num_processes: int) -> None:
    start = time.perf_counter()

    all_files = []
    for root, _, files in os.walk("dataset"):
        for name in files:
            all_files.append(os.path.join(root, name))
    all_files.sort()

    chunk_size = len(all_files) // num_processes
    chunks = [all_files[i:i + chunk_size] for i in range(0, len(all_files), chunk_size)]

    with Pool(processes=num_processes) as pool:
        results = pool.map(process_file_chunk, chunks)

    final_counts = Counter()
    for res in results:
        final_counts.update(res)

    os.makedirs("final-output", exist_ok=True)
    with open("final-output/answer.txt", "w", encoding="utf-8") as f:
        for word, count in sorted(final_counts.items()):
            f.write(f"{word} {count}\n")
            
    end = time.perf_counter() - start
    print(f"Time taken for multiprocess.py ({num_processes} procs): {end:.4f} s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num",
        dest="num_processes",
        type=int,
        default=2,
        help="Number of worker processes to use.",
    )
    args = parser.parse_args()
    main(args.num_processes)
