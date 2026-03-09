from abc import ABC, abstractmethod


class MapReduceJob(ABC):
    name = ""

    @abstractmethod
    def prepare_input(self, input_handle, byte_start, byte_end, file_size):
        raise NotImplementedError

    @abstractmethod
    def map(self, map_task_id, input_value):
        raise NotImplementedError

    def combine(self, pairs):
        return pairs

    @abstractmethod
    def reduce(self, key, values):
        raise NotImplementedError

    @abstractmethod
    def serialize_intermediate(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def deserialize_intermediate(self, line):
        raise NotImplementedError

    @abstractmethod
    def format_output(self, key, value):
        raise NotImplementedError
