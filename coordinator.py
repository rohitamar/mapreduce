import argparse
import asyncio
import grpc
import os
import shutil
import time

from proto import mapreduce_pb2
from proto import mapreduce_pb2_grpc

from google.protobuf import empty_pb2

DATASET_PATH = "./dataset"
RUNTIME_DIRS = ("./dump", "./output")
WORKER_PORT = 50051
READINESS_TIMEOUT_SECONDS = 30


def chunker(chunk_size=16 * 1024 * 1024):
    map_task_id = 0
    for name in sorted(os.listdir(DATASET_PATH)):
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


def get_worker_target(worker):
    return f"{worker['host']}:{worker['port']}"

def reset_runtime_dirs():
    for runtime_dir in RUNTIME_DIRS:
        os.makedirs(runtime_dir, exist_ok=True)
        for entry in os.scandir(runtime_dir):
            path = entry.path
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(path)
            else:
                os.remove(path)


async def wait_for_worker(worker):
    target = get_worker_target(worker)
    deadline = time.monotonic() + READINESS_TIMEOUT_SECONDS
    last_error = None

    while time.monotonic() < deadline:
        try:
            async with grpc.aio.insecure_channel(target) as channel:
                await asyncio.wait_for(channel.channel_ready(), timeout=1)
                return
        except Exception as e:
            last_error = e
            await asyncio.sleep(0.5)

    raise TimeoutError(f"Worker {target} was not ready: {last_error}")

async def wait_for_workers():
    print("Waiting for workers to become ready...")
    await asyncio.gather(*(wait_for_worker(worker) for worker in worker_metadata))
    print("All workers are ready.")

async def map_worker(queue, worker):
    async with grpc.aio.insecure_channel(get_worker_target(worker)) as channel:
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
                    assigned_worker_id=worker["worker_id"],
                    num_reduce_partitions=NUM_REDUCE_PARTITIONS,
                    job_name=JOB_NAME,
                    partitioner_name=PARTITIONER_NAME,
                ))
            except Exception as e:
                print(e)
            finally:
                queue.task_done()

async def finalize_map_worker(worker):
    async with grpc.aio.insecure_channel(get_worker_target(worker)) as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.FinalizeMap(empty_pb2.Empty())
        except Exception as e:
            print(e)

async def reduce_worker(worker, mapper_worker_ids, reduce_partition_id):
    async with grpc.aio.insecure_channel(get_worker_target(worker)) as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        try:
            await stub.Reduce(mapreduce_pb2.ReduceRequest(
                mapper_worker_ids=mapper_worker_ids,
                assigned_worker_id=worker["worker_id"],
                reduce_partition_id=reduce_partition_id,
                job_name=JOB_NAME,
                partitioner_name=PARTITIONER_NAME,
            ))
        except Exception as e:
            print(e)

async def end_phase_worker(worker):
    async with grpc.aio.insecure_channel(get_worker_target(worker)) as channel:
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
    reset_runtime_dirs()
    await wait_for_workers()
    print("Starting map phase.")

    # mapper phase
    queue = asyncio.Queue(maxsize=2 * W)
    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(map_worker(
                queue,
                worker
            ))
        )
    
    # chunk dataset
    await produce_chunks(queue)

    # done consuming chunks, let workers know
    for _ in range(W):
        await queue.put(None)

    await asyncio.gather(*workers)
    print("Map phase complete. Finalizing mapper output.")

    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(finalize_map_worker(worker))
        )

    await asyncio.gather(*workers)
    print("Starting reduce phase.")

    # reducer phase
    mapper_worker_ids = [worker["worker_id"] for worker in worker_metadata]
    workers = []
    for reduce_partition_id in range(NUM_REDUCE_PARTITIONS):
        worker = worker_metadata[reduce_partition_id % W]
        workers.append(
            asyncio.create_task(reduce_worker(
                worker,
                mapper_worker_ids,
                reduce_partition_id,
            ))
        )
    
    await asyncio.gather(*workers)
    print("Reduce phase complete. Stopping workers.")

    workers = []
    for worker in worker_metadata:
        workers.append(
            asyncio.create_task(end_phase_worker(worker))
        )

    await asyncio.gather(*workers)

    elapsed_time = time.perf_counter() - start_time
    print(f"MapReduce job completed in {elapsed_time:.2f} seconds")
    
if __name__ == '__main__':
    global JOB_NAME, PARTITIONER_NAME, worker_metadata, W, NUM_REDUCE_PARTITIONS

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", default="word_count")
    parser.add_argument("--partitioner", default="crc32")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--worker-host-prefix", default="worker")
    args = parser.parse_args()

    JOB_NAME = args.job
    PARTITIONER_NAME = args.partitioner
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")

    worker_metadata = [
        {
            "host": f"{args.worker_host_prefix}{worker_id}",
            "port": WORKER_PORT,
            "worker_id": worker_id,
        }
        for worker_id in range(args.workers)
    ]

    W = len(worker_metadata)
    NUM_REDUCE_PARTITIONS = W
    asyncio.run(main())
