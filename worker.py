import asyncio
import argparse
import heapq
import itertools
import grpc 
import mapreduce_pb2_grpc
import os 
import sys 

from google.protobuf import empty_pb2
from contextlib import ExitStack
from utils.buffer_manager import BufferManager
from utils.jobs import JobFactory
from utils.partitioners import PartitionerFactory

class MapReduceServicer(mapreduce_pb2_grpc.MapReduceServicer):
    
    def __init__(self):
        self.buffer_manager = None
        self.job = None
        self.job_name = None
        self.partitioner_name = None
        self.server = None

    def set_server(self, server):
        self.server = server

    async def _shutdown_server(self):
        # Let the EndPhase response flush before beginning graceful shutdown.
        await asyncio.sleep(0)
        if self.server is not None:
            await self.server.stop(1)

    def configure(self, job_name, partitioner_name):
        if self.job_name == job_name and self.partitioner_name == partitioner_name:
            return

        if self.buffer_manager is not None:
            raise RuntimeError(
                "Cannot change job or partitioner while a map phase is still active"
            )

        self.job = JobFactory.create(job_name)
        self.job_name = job_name
        self.partitioner_name = partitioner_name

    async def Map(self, request, context):
        self.configure(request.job_name, request.partitioner_name)

        assigned_worker_id = request.assigned_worker_id
        input_file = request.input_path
        byte_start = request.byte_start
        byte_end = request.byte_end
        num_reduce_partitions = request.num_reduce_partitions

        if self.buffer_manager is None:
            self.buffer_manager = BufferManager(
                worker_id=assigned_worker_id, 
                num_buckets=num_reduce_partitions,
                partitioner=PartitionerFactory.create(
                    self.partitioner_name,
                    num_reduce_partitions,
                ),
                serializer=self.job.serialize_intermediate,
                dump_dir="./dump",
                buffer_threshold_bytes=16 * 1024 * 1024 # 192 KB
            )

        try:
            with open(input_file, 'rb') as input_handle:
                input_handle.seek(0, os.SEEK_END)
                file_size = input_handle.tell()
                input_value = self.job.prepare_input(
                    input_handle=input_handle,
                    byte_start=byte_start,
                    byte_end=byte_end,
                    file_size=file_size,
                )

                self.buffer_manager.write_pairs(
                    self.job.combine(
                        self.job.map(request.map_task_id, input_value)
                    )
                )

        except IOError as e:
            print(e)
            
        return empty_pb2.Empty()
    
    async def FinalizeMap(self, request, context):
        if self.buffer_manager is None:
            return empty_pb2.Empty()

        self.buffer_manager.flush()

        worker_id = self.buffer_manager.worker_id
        num_buckets = self.buffer_manager.num_buckets
        num_runs = self.buffer_manager.get_num_flushes

        for bucket in range(num_buckets):
            final_file = f"./dump/map-{worker_id}-partition-{bucket}"
            if num_runs == 1:
                run_file = f"./dump/map-{worker_id}-partition-{bucket}-run-0"
                if os.path.exists(run_file):
                    os.replace(run_file, final_file)
                continue

            with ExitStack() as stack:
                sorted_runs = []
                for run_idx in range(num_runs):
                    run_file = f"./dump/map-{worker_id}-partition-{bucket}-run-{run_idx}"
                    if not os.path.exists(run_file):
                        continue 

                    handle = stack.enter_context(open(run_file, "r", encoding="utf-8"))
                    sorted_runs.append(
                        map(self.job.deserialize_intermediate, handle)
                    )
                
                if not sorted_runs:
                    continue 

                merged = heapq.merge(*sorted_runs, key=lambda item: item[0])
                with open(final_file, "w", encoding="utf-8") as out:
                    for key, value in merged:
                        out.write(f"{key}\t{value}\n")

        self.buffer_manager = None
        return empty_pb2.Empty()
    
    def shuffle_and_sort(self, reduce_partition_id, mapper_worker_ids):        
        stack = ExitStack()
        reducer_files = []
        for mapper_worker_id in mapper_worker_ids:
            reducer_file_name = f"./dump/map-{mapper_worker_id}-partition-{reduce_partition_id}"
            if not os.path.exists(reducer_file_name):
                continue 

            f = stack.enter_context(open(reducer_file_name, 'r', encoding='utf-8'))
            reducer_files.append(map(self.job.deserialize_intermediate, f))
        
        if not reducer_files:
            return iter(()), stack 
        
        merged = heapq.merge(*reducer_files, key = lambda f : f[0])
        grouped = itertools.groupby(merged, key=lambda f: f[0])
        return grouped, stack

    async def Reduce(self, request, context):
        self.configure(request.job_name, request.partitioner_name)

        reducer_input, stack = self.shuffle_and_sort(
            request.reduce_partition_id,
            request.mapper_worker_ids,
        )
        with open(f'./output/reducer-{request.reduce_partition_id}', 'w', encoding='utf-8') as f:
            for key, group in reducer_input:
                _, reduced_value = self.job.reduce(
                    key,
                    (value for _, value in group),
                )
                f.write(self.job.format_output(key, reduced_value))

        stack.close()
        return empty_pb2.Empty()

    async def EndPhase(self, request, context):
        if self.server is not None:
            asyncio.create_task(self._shutdown_server())
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
    parser = argparse.ArgumentParser()
    parser.add_argument("port", type=int)
    args = parser.parse_args(sys.argv[1:])
    asyncio.run(serve(args.port))
    
