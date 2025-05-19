from xmlrpc.server import SimpleXMLRPCServer
server = SimpleXMLRPCServer(("localhost", 8000), logRequests=True)

workers = {}

def register_worker():
    idx = len(workers)
    workers[idx] = {
        id: idx
    }
    return idx

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

ptr = iter(chunker(1))
def get_next_chunk():
    return next(ptr)

if __name__ == '__main__':
    print(repr(get_next_chunk()))
    print(repr(get_next_chunk()))

    coordinator_server = SimpleXMLRPCServer(("localhost", 8000), logRequests=True)
    
    coordinator_server.register_function(register_worker, 'register_worker')
    coordinator_server.register_function(get_next_chunk, 'get_next_chunk')

    coordinator_server.serve_forever()

    # connect to each worker (how will I know who the workers are?? IP + port)
    # save any data for each worker (IP, port, idx, status)
    # now you have all workers.
    # using RPC in coordinator, give each worker a chunk (get_next_chunk, pass into mapper_func)
    # when the worker finishes the chunk, the worker RPCs the coordinator to give the next chunk?
    # this process repeats until all chunks have been mapped.

    # note that each worker is writing to /dump/map-{worker_id}-spill-i (where 0 <= i <= R)
    # after this, we need to do the shuffle + sort phase.

