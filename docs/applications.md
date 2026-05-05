# Word Count

```
map(doc_id, text):
    for word in text:
        yield (word, 1)

reduce(word, freqs):
    yield (word, sum(freqs))
```

# Sorting
```
Assume each file is just a bunch of numbers

map(doc_id, value):
    yield value

reducer(_, values):
    yield values

Use range based partitioning. Hash based partitioning won't work at the mapper phase's buffer_manager flushing/sorting step. 
```

# Average Movie Rating
```
Assume each file is of format "{movie_name},{rating}"

map(doc_id, line):
    movie_name, rating = line 
    yield (movie_name, (rating, 1))

reduce(key, rating_and_counts):
    tot_r = tot_c = 0
    for r, c in rating_and_counts:
        tot_r += r
        tot_c += c
    
    yield (key, tot_r / tot_c)
```

# Max City Temperature
```
Assume each file is of format "{city},{temperature}"

map(doc_id, line):
    city, temperature = line 
    yield (city, temperature)

reduce(city, temperatures):
    yield (city, max(temperatures))
```

# Inverted Index
```
Each file contains many words

map(doc_id, line):
    for word in line:
        yield (word, doc_id)

reduce(word, docs):
    yield (word, set(docs))
```