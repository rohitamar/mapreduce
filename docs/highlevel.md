# Mapper Phase 

1. The coordinator node uses `chunker(chunk_size)` to build chunks. Each chunk is a `(map_task_id, input_path, byte_start, byte_end)` tuple and is randomly given to a worker node.
2. In each worker node, the first call to `async def Map` calls `self.configure`, which sets up `self.buffer_manager`. The `buffer_manager` stores intermediate mapper values that have not been flushed to disk. If each string that is mapped is written to disk, this will be very slow. Instead, we wait for a certain threshold (`buffer_thresold_bytes=64MB`) and write to disk altogether. 
3. There is also an intermediary `combine` function that is used after the map function. Think of it as a mini-reducer that compresses mapper output within the node. 
4. The structure of the above is `map-{worker_id}-partition-{bucket_id}-run-{num_flushes}`. Once the mapper node is done with all chunks that the coordinator gave it, the `FinalizeMap` function merges `run-{i}` for a `(worker_id, bucket_id)`. The number of partitions is the number of reducers nodes we have. The variable `worker_id` is determined by the number of worker nodes we'll have.

# Shuffle and Sort
1. Take every file `map-{i}-partition-{reducer_id}`. Merge these files. 

The **shuffling** comes from the mapper nodes partitioning the data into `num_reducer` slots. The **sorting** comes from when we merge each individual mapper node's data. Note that sorting happens in two places: once in the mapper node itself (during buffer flushes) and another when merging each reducer partition from all mapper nodes. 

Note that there is a key difference in my implementation in this step. Generally, MapReduce implementations would need to pull files from a shared distributed file system when moving from mapper to reducer phase. Here, we were able to skip that. In the future, we need to simulate this as well.

# Reducer Phase
1. The shuffle and sort phase returns a heap-sorted generator (see L147 `reducer_input, stack` in file `worker.py`). 
2. With L147 `itertools.groupby` in `shuffle_and_sort`, the input for the reducer phase is already set up. A call to `self.job.reduce` is made to perform the necessary transformation on the data. 
