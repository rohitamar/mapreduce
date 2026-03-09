import heapq
import os
import shutil
import tempfile
from contextlib import ExitStack

OUTPUT_DIR = "./output"
DUMP_DIR = "./dump"
MERGED_FILE_NAME = "merged-output"

def parse_output_line(line):
    key, count = line.rstrip("\n").rsplit(" ", 1)
    return key, int(count)

def iter_output_file(file_handle):
    for line in file_handle:
        if line.strip():
            yield parse_output_line(line)

def clear_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        return

    for entry in os.listdir(directory):
        entry_path = os.path.join(directory, entry)
        if os.path.isdir(entry_path):
            shutil.rmtree(entry_path)
        else:
            os.remove(entry_path)

def merge_outputs():
    reducer_files = sorted(
        os.path.join(OUTPUT_DIR, name)
        for name in os.listdir(OUTPUT_DIR)
        if os.path.isfile(os.path.join(OUTPUT_DIR, name))
    )

    if not reducer_files:
        raise FileNotFoundError("No reducer output files found in ./output")

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=tempfile.gettempdir(),
    ) as temp_output:
        temp_output_path = temp_output.name

        with ExitStack() as stack:
            streams = []
            for reducer_file in reducer_files:
                handle = stack.enter_context(open(reducer_file, "r", encoding="utf-8"))
                streams.append(iter_output_file(handle))

            merged = heapq.merge(*streams, key=lambda item: item[0])
            for key, count in merged:
                temp_output.write(f"{key} {count}\n")

    clear_directory(DUMP_DIR)
    clear_directory(OUTPUT_DIR)

    final_output_path = os.path.join(OUTPUT_DIR, MERGED_FILE_NAME)
    shutil.move(temp_output_path, final_output_path)
    return final_output_path

if __name__ == "__main__":
    merged_path = merge_outputs()
    print(f"Merged output written to {merged_path}")
