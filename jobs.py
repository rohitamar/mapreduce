import re
from MapReduceJob import MapReduceJob

class WordCounterJob(MapReduceJob):
    def map(key, value):
        for word in re.findall(r"\b\w+\b", value.lower()):
            yield (word, 1)

    def reduce(key, values):
        return key, sum(values)
