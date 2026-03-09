import asyncio 
import grpc
import os
import time
import mapreduce_pb2
import mapreduce_pb2_grpc

from google.protobuf import empty_pb2

DATASET_PATH = "./dataset"

def chunker(chunk_size= 8*1024*1024):                                                                                                                                                      
    for name in os.listdir(DATASET_PATH):                                                                                                                                                                      
        file_path = os.path.join(DATASET_PATH, name)                                                                                                                                                           
        if not os.path.isfile(file_path):                                                                                                                                                                     
            continue                                                                                                                                                                                          
                                                                                                                                                                                                            
        try:                                                                                                                                                                                                  
            file_size = os.path.getsize(file_path)                                                                                                                                                            
            start = 0                                                                                                                                                                                         
            while start < file_size:                                                                                                                                                                          
                end = min(start + chunk_size, file_size)                                                                                                                                                      
                yield (file_path, start, end)                                                                                                                                                                 
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
            file_path, start, end = byte_range
            try: 
                await stub.Map(mapreduce_pb2.MapRequest(
                    key=f"{start}:{end}",
                    value=file_path,
                    worker_id=worker_id,
                    num_workers=W
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

async def reduce_worker(port, worker_ids, worker_id):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.Reduce(mapreduce_pb2.ReduceRequest(
                worker_ids=worker_ids,
                worker_id=worker_id
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
    worker_ids = [w['worker_id'] for w in worker_metadata]
    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(reduce_worker(
                worker['port'],
                worker_ids,
                worker['worker_id']
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
    global worker_metadata, W

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
    asyncio.run(main())
