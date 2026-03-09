import asyncio
import heapq
import itertools
import grpc 
import mapreduce_pb2_grpc
import os 
import sys 

from google.protobuf import empty_pb2
from contextlib import ExitStack
from BufferManager import BufferManager
from jobs import WordCounterJob

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServicer):
    
    def __init__(self):
        self.buffer_manager = None
        self.server = None

    def set_server(self, server):
        self.server = server

    def trim(self, input_handle, data, byte_start, byte_end, file_size):
        if byte_start > 0:
            input_handle.seek(byte_start - 1)
            prev = input_handle.read(1)
            if prev and not prev.isspace():
                first_ws = -1
                for i, ch in enumerate(data):
                    if bytes([ch]).isspace():
                        first_ws = i
                        break
                data = b'' if first_ws == -1 else data[first_ws + 1:]

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

    async def Map(self, request, context):
        worker_id = request.worker_id
        input_file = request.value
        byte_start, byte_end = map(int, request.key.split(":"))
        R = request.num_workers

        if self.buffer_manager is None:
            self.buffer_manager = BufferManager(
                worker_id=worker_id, 
                num_buckets=R,
                dump_dir="./dump",
                buffer_threshold_bytes=192 * 1024 # 192 KB
            )

        try:
            with open(input_file, 'rb') as input_handle:
                input_handle.seek(0, os.SEEK_END)
                file_size = input_handle.tell()

                input_handle.seek(byte_start)
                data = input_handle.read(byte_end - byte_start)

                data = self.trim(
                    input_handle=input_handle,
                    data=data,
                    byte_start=byte_start,
                    byte_end=byte_end,
                    file_size=file_size,
                )

                data = data.decode('utf-8', errors='ignore')
                self.buffer_manager.write_pairs(
                    WordCounterJob.map(request.key, data)
                )

        except IOError as e:
            print(e)
            
        return empty_pb2.Empty()

    def parse_line(self, line):
        word, count = line.rstrip("\n").split("\t")
        return word, int(count)
    
    async def FinalizeMap(self, request, context):
        if self.buffer_manager is None:
            return empty_pb2.Empty()

        self.buffer_manager.flush()

        worker_id = self.buffer_manager.worker_id
        num_buckets = self.buffer_manager.num_buckets
        num_runs = self.buffer_manager.get_num_flushes

        for bucket in range(num_buckets):
            final_file = f"./dump/map-{worker_id}-spill-{bucket}"
            with ExitStack() as stack:
                sorted_runs = []
                for run_idx in range(num_runs):
                    run_file = f"./dump/map-{worker_id}-spill-{bucket}-run-{run_idx}"
                    if not os.path.exists(run_file):
                        continue 

                    handle = stack.enter_context(open(run_file, "r", encoding="utf-8"))
                    sorted_runs.append(
                        map(self.parse_line, handle)
                    )
                
                if not sorted_runs:
                    continue 

                merged = heapq.merge(*sorted_runs, key=lambda item: item[0])
                with open(final_file, "w", encoding="utf-8") as out:
                    for key, value in merged:
                        out.write(f"{key}\t{value}\n")

        self.buffer_manager = None
        return empty_pb2.Empty()
    
    def shuffle_and_sort(self, worker_id, worker_ids):        
        stack = ExitStack()
        reducer_files = []
        for wid in worker_ids:
            reducer_file_name = f"./dump/map-{wid}-spill-{worker_id}"
            if not os.path.exists(reducer_file_name):
                continue 

            f = stack.enter_context(open(reducer_file_name, 'r', encoding='utf-8'))
            reducer_files.append(map(self.parse_line, f))
        
        if not reducer_files:
            return iter(()), stack 
        
        merged = heapq.merge(*reducer_files, key = lambda f : f[0])
        grouped = itertools.groupby(merged, key=lambda f: f[0])
        return grouped, stack

    async def Reduce(self, request, context):
        reducer_input, stack = self.shuffle_and_sort(request.worker_id, request.worker_ids)
        with open(f'./output/reducer-{request.worker_id}', 'w') as f:
            for key, group in reducer_input:
                _, word_count = WordCounterJob.reduce(
                    key,
                    (count for _, count in group),
                )
                f.write(f"{key} {word_count}\n")

        stack.close()
        return empty_pb2.Empty()

    async def EndPhase(self, request, context):
        if self.server is not None:
            asyncio.create_task(self.server.stop(0))
        return empty_pb2.Empty()

async def serve(port):
    server = grpc.aio.server()
    servicer = MapReduceServicer()
    servicer.set_server(server)
    mapreduce_pb2_grpc.add_MapReduceServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    await server.start() 
    print(f"Server starting on {port}")
    await server.wait_for_termination()

if __name__ == '__main__':
    port = int(sys.argv[1])
    asyncio.run(serve(port))
    
