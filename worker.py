import asyncio
import grpc 
import mapreduce_pb2 
import mapreduce_pb2_grpc
import sys 

from google.protobuf import empty_pb2

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
    
    def pre_reduce(self):
        
    async def reduce(self, request, context):
        # shuffle: open len(worker_ids) many files, I think we'll send the worker_ids
        #  

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
    