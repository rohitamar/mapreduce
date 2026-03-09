import os
import re
import time
from collections import Counter

def main():
    start = time.perf_counter()
    counts = Counter()
    for root, _, files in os.walk("dataset"):
        files.sort()
        for name in files:
            path = os.path.join(root, name)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    counts.update(re.findall(r"\b\w+\b", line.lower()))

    with open("final-output/answer.txt", "w", encoding="utf-8") as f:
        for word, count in sorted(counts.items()):
            f.write(f"{word} {count}\n")
    end = time.perf_counter() - start
    print(f"Time taken for manual.py: {end} s")

if __name__ == "__main__":
    main()
