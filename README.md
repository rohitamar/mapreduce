# MapReduce

## Introduction

Simplified implementation of [Mapreduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) in Python. Repository is for the word frequency task, but is meant to be easily changed for other tasks that follow the mapreduce pattern.

## Usage

```docker compose up --build```

Then, run `make merge`. To verify, run `make single` and `make eq`. 

## Results

![docker desktop results logs](https://github.com/rohitamar/mapreduce/blob/main/img/logs.png)

## Future

Currently, the docker instances assume the existence of a shared folder (./dump, ./output, ./dataset). An improvement, while following the pattern suggested in the paper, would have these folders in a NFS, and each docker node is communicating with the NFS. 
