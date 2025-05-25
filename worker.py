import asyncio
import heapq
import itertools
import grpc 
import mapreduce_pb2 
import mapreduce_pb2_grpc
import sys 

from google.protobuf import empty_pb2
from contextlib import ExitStack

R = 6

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServicer):
    async def Map(self, request, context):
        worker_id = request.worker_id
        value = request.value
        try:
            files = [open(f"./dump/map-{worker_id}-spill-{i}") for i in range(R)]
            for word in value.split(" "):
                hsh = hash(word) % R 
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
            f = stack.enter_context(open(f"./dump/map-{wid}-spill-{worker_id}"))
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
    