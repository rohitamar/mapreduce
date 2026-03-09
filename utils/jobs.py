import re
from collections import Counter

from utils.map_reduce_job import MapReduceJob

class WordCounterJob(MapReduceJob):
    name = "word_count"

    def _trim_to_word_boundaries(self, input_handle, data, byte_start, byte_end, file_size):
        if byte_start > 0:
            input_handle.seek(byte_start - 1)
            prev = input_handle.read(1)
            if prev and not prev.isspace():
                first_ws = -1
                for i, ch in enumerate(data):
                    if bytes([ch]).isspace():
                        first_ws = i
                        break
                data = b"" if first_ws == -1 else data[first_ws + 1 :]

        if byte_end < file_size and data and not bytes([data[-1]]).isspace():
            input_handle.seek(byte_end)
            while True:
                ch = input_handle.read(1)
                if not ch:
                    break
                data += ch
                if ch.isspace():
                    break

        return data

    def prepare_input(self, input_handle, byte_start, byte_end, file_size):
        input_handle.seek(byte_start)
        data = input_handle.read(byte_end - byte_start)
        data = self._trim_to_word_boundaries(
            input_handle=input_handle,
            data=data,
            byte_start=byte_start,
            byte_end=byte_end,
            file_size=file_size,
        )
        return data.decode("utf-8", errors="ignore")

    def map(self, map_task_id, input_value):
        del map_task_id
        for word in re.finditer(r"\b\w+\b", input_value.lower()):
            yield word.group(), 1

    def combine(self, pairs):
        counts = Counter()
        for key, value in pairs:
            counts[key] += value

        for key, value in counts.items():
            yield key, value

    def reduce(self, key, values):
        return key, sum(values)

    def serialize_intermediate(self, key, value):
        return f"{key}\t{value}\n"

    def deserialize_intermediate(self, line):
        word, count = line.rstrip("\n").split("\t", 1)
        return word, int(count)

    def format_output(self, key, value):
        return f"{key} {value}\n"

class JobFactory:
    @staticmethod 
    def create(job_type: str) -> MapReduceJob:
        if job_type == "word_count":
            return WordCounterJob()
        raise ValueError(f"Unknown job_type: {job_type}")
