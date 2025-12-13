import asyncio 
import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc

def chunker(num_lines=2000):
    try: 
        with open('small.txt', 'r') as f:
            chunk = []
            for line in f:
                chunk.append(line.strip())
                if len(chunk) == num_lines:
                    yield ' '.join(chunk)
                    chunk = []
    except Exception as e:
        print(f"Error: {e}")

async def map_worker(queue, port, worker_id):
    async with grpc.aio.insecure_channel(f'localhost:{port}') as channel:
        stub = mapreduce_pb2_grpc.MapReduceStub(channel)
        while True:
            chunk = await queue.get()
            if chunk is None:
                queue.task_done()
                break 
            try: 
                await stub.Map(mapreduce_pb2.MapRequest(
                    value=chunk,
                    key=chunk,
                    worker_id=worker_id,
                    num_workers=W
                ))
            except Exception as e:
                print(e)
            finally:
                queue.task_done()

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

ptr = iter(chunker(3))
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

    # connect to each worker (how will I know who the workers are?? IP + port)
    # save any data for each worker (IP, port, idx, status)
    # now you have all workers.
    
    # using RPC in coordinator, give each worker a chunk (get_next_chunk, pass into mapper_func)
    # when the worker finishes the chunk, the worker RPCs the coordinator to give the next chunk?
    # this process repeats until all chunks have been mapped.

    # note that each worker is writing to /dump/map-{worker_id}-spill-i (where 0 <= i <= R)
    # after this, we need to do the shuffle + sort phase.

