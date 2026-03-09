import re
from MapReduceJob import MapReduceJob

class WordCounterJob(MapReduceJob):
    def map(key, value):
        for word in re.finditer(r"\b\w+\b", value.lower()):
            yield (word.group(), 1)

    def reduce(key, values):
        return key, sum(values)
