from mr import map, reduce
import xmlrpc.client

R = 6

def mapper_func(key, value, worker_id):
    try:
        files = [open(f"./dump/map-{worker_id}-spill-{i}", 'w') for i in range(R)]
        for k, v in map(key, value):
            hsh = hash(k) % R
            files[hsh].write(f"{k}\t{v}\n")
    except IOError as e:
        print("??")
    finally:
        for file in files:
            file.close()

def reducer_func(key, value, worker_id):
    try: 
        files = [open(f"./dump/map-{idx}-spill-{worker_id}") for idx in workers.keys()]

    except IOError:
        print("???")

if __name__ == '__main__':

    worker_server = SimpleXMLRPCServer(("localhost"))