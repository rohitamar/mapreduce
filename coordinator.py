import asyncio 
import grpc
import os
import time
import mapreduce_pb2
import mapreduce_pb2_grpc

from google.protobuf import empty_pb2

DATASET_PATH = "./dataset"

def chunker(chunk_size= 8*1024*1024):                                                                                                                                                      
    map_task_id = 0
    for name in os.listdir(DATASET_PATH):                                                                                                                                                                      
        file_path = os.path.join(DATASET_PATH, name)                                                                                                                                                           
        if not os.path.isfile(file_path):                                                                                                                                                                     
            continue                                                                                                                                                                                          
                                                                                                                                                                                                            
        try:                                                                                                                                                                                                  
            file_size = os.path.getsize(file_path)                                                                                                                                                            
            start = 0                                                                                                                                                                                         
            while start < file_size:                                                                                                                                                                          
                end = min(start + chunk_size, file_size)                                                                                                                                                      
                yield {
                    "map_task_id": map_task_id,
                    "input_path": file_path,
                    "byte_start": start,
                    "byte_end": end,
                }
                map_task_id += 1
                start = end                                                                                                                                                                                   
        except Exception as e:                                                                                                                                                                                
            print(f"Error with {file_path}: {e}") 

async def map_worker(queue, port, worker_id):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        while True:
            byte_range = await queue.get()
            if byte_range is None:
                queue.task_done()
                break 
            try: 
                await stub.Map(mapreduce_pb2.MapRequest(
                    map_task_id=byte_range["map_task_id"],
                    input_path=byte_range["input_path"],
                    byte_start=byte_range["byte_start"],
                    byte_end=byte_range["byte_end"],
                    assigned_worker_id=worker_id,
                    num_reduce_partitions=NUM_REDUCE_PARTITIONS,
                ))
            except Exception as e:
                print(e)
            finally:
                queue.task_done()

async def finalize_map_worker(port):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.FinalizeMap(empty_pb2.Empty())
        except Exception as e:
            print(e)

async def reduce_worker(port, mapper_worker_ids, assigned_worker_id, reduce_partition_id):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.Reduce(mapreduce_pb2.ReduceRequest(
                mapper_worker_ids=mapper_worker_ids,
                assigned_worker_id=assigned_worker_id,
                reduce_partition_id=reduce_partition_id,
            ))
        except Exception as e:
            print(e)

async def end_phase_worker(port):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.EndPhase(empty_pb2.Empty())
        except Exception as e:
            print(e)

ptr = iter(chunker())
def get_next_chunk():
    return next(ptr)

async def produce_chunks(queue):
    while True:
        try:
            chunk = get_next_chunk()
        except StopIteration:
            break 
        await queue.put(chunk)

async def main():
    start_time = time.perf_counter()
    # mapper phase
    queue = asyncio.Queue(maxsize = 2 * W)
    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(map_worker(
                queue,
                worker['port'],
                worker['worker_id']
            ))
        )
    
    # chunk dataset
    await produce_chunks(queue)

    # done consuming chunks, let workers know
    for _ in range(W):
        await queue.put(None)

    await asyncio.gather(*workers)

    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(finalize_map_worker(worker['port']))
        )

    await asyncio.gather(*workers)

    # reducer phase
    mapper_worker_ids = [worker["worker_id"] for worker in worker_metadata]
    workers = []
    for reduce_partition_id in range(NUM_REDUCE_PARTITIONS):
        worker = worker_metadata[reduce_partition_id % W]
        workers.append(
            asyncio.create_task(reduce_worker(
                worker['port'],
                mapper_worker_ids,
                worker['worker_id'],
                reduce_partition_id,
            ))
        )
    
    await asyncio.gather(*workers)

    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(end_phase_worker(worker['port']))
        )

    await asyncio.gather(*workers)

    elapsed_time = time.perf_counter() - start_time
    print(f"MapReduce job completed in {elapsed_time:.2f} seconds")
    
if __name__ == '__main__':
    global worker_metadata, W, NUM_REDUCE_PARTITIONS

    worker_metadata = []
    with open('metadata.txt', 'r') as f:
        for line in f.readlines():
            port, worker_id = line.split(",")
            port, worker_id = int(port), int(worker_id)
            worker_metadata.append({
                'port': port, 
                'worker_id': worker_id
            })

    W = len(worker_metadata)
    NUM_REDUCE_PARTITIONS = W
    asyncio.run(main())
