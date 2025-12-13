import asyncio
import hashlib
import heapq
import itertools
import grpc 
import mapreduce_pb2 
import mapreduce_pb2_grpc
import os 
import sys 

from google.protobuf import empty_pb2
from contextlib import ExitStack

SPLIT_FILE_LEN = 30_000

def split_and_sort_file(file_name):
    contents = []
    split_files = []
    with open(file_name, 'r') as f:
        for line in f:
            word, count = line.split("\t")
            count = int(count)
            contents.append((word, count))

            if len(contents) == SPLIT_FILE_LEN:
                contents.sort()
                split_count = len(split_files)
                split_file_name = f"{file_name}-split-{split_count}"
                split_files.append(split_file_name)
                with open(split_file_name, 'w') as splitter:
                    for word, count in contents:
                        splitter.write(f"{word}\t{count}\n")
                contents = []

        contents.sort()
        split_count = len(split_files)
        split_file_name = f"{file_name}-split-{split_count}"
        split_files.append(split_file_name)
        with open(split_file_name, 'w') as splitter:
            for word, count in contents:
                splitter.write(f"{word}\t{count}\n")
        contents = []

    # delete the file with "filename"??
    # os.remove(file_name)
    return split_files

def sha_to_bucket(key, R):
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    big_int = int.from_bytes(digest, byteorder="big")
    return big_int % R

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServicer):
    async def Map(self, request, context):
        worker_id = request.worker_id
        value = request.value
        R = request.num_workers
        try:
            files = [
                open(f"./dump/map-{worker_id}-spill-{i}", 'a') 
                for i in range(R)
            ]
            for word in value.split(" "):
                if not word:
                    continue 
                hsh = sha_to_bucket(word, R)
                files[hsh].write(f"{word}\t1\n")
        except IOError as e:
            print(e)
        finally:
            for file in files:
                file.close()
        return empty_pb2.Empty()
    
    def pre_reduce(self, worker_id, worker_ids):
        def parse(line):
            word, count = line.split("\t")
            return word, int(count) 
        
        stack = ExitStack()
        reducer_files = []
        for wid in worker_ids:
            split_file_names = split_and_sort_file(f"./dump/map-{wid}-spill-{worker_id}")
            print(split_file_names)
            for split_file_name in split_file_names: 
                f = stack.enter_context(open(split_file_name, 'r'))
                reducer_files.append(map(parse, f))
        
        merged = heapq.merge(*reducer_files, key = lambda f : f[0])
        grouped = itertools.groupby(merged, key=lambda f: f[0])
        return grouped, stack

    async def Reduce(self, request, context):
        # for reduce, you just need the worker_ids and this process's worker_id
        # shuffle: open len(worker_ids) many files, I think we'll send the worker_ids
        # sort + merge (will be done in pre_reduce)
        reducer_input, stack = self.pre_reduce(request.worker_id, request.worker_ids)
        # now reduce:
        # merged is an iterable of type (key, list(value))
        # we need to return list(v2)
        with open(f'./output/reducer-{request.worker_id}', 'w') as f:
            for key, group in reducer_input:
                word_count = sum(count for _, count in group)
                f.write(f"{key} {word_count}\n")
        stack.close()
        return empty_pb2.Empty()

async def serve(port):
    server = grpc.aio.server()
    mapreduce_pb2_grpc.add_MapReduceServicer_to_server(MapReduceServicer(), server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start() 
    print(f"Server starting on {port}")
    await server.wait_for_termination()

if __name__ == '__main__':
    port = int(sys.argv[1])
    asyncio.run(serve(port))
    