def map(key: str, value: str):
    for word in value.split(" "):
        yield (word, 1)

def reduce(key, values):
    pass 
    
