from abc import ABC, abstractmethod

class MapReduceJob(ABC):

    @abstractmethod 
    def map(key, value):
        pass

    @abstractmethod
    def reduce(key, values):
        pass 
    
