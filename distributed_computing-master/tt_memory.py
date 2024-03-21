import logging
import time
import pandas as pd
import numpy as np
import ray
import time
import copy
import random
import math


@ray.remote
def func(df_data, col_id):
    col_name = f'b{id}'
    new_df_data = pd.DataFrame(np.zeros((df_data.shape[0], 1), ), columns=[col_name])
    for i in range(df_data.shape[0]):
        new_df_data.loc[i, col_name] = col_id
    result = new_df_data.loc[df_data.shape[0] - 1, col_name]
    new_id = ray.put(new_df_data)
    return new_id


if __name__ == '__main__':
    NUM_TASK = 360
    DATA_SIZE = 30000
    COL_SIZE = 50
    NUM_KERNELS = 30
    ALL_ROUNDS = math.ceil(NUM_TASK / NUM_KERNELS)
    logging.getLogger().setLevel(logging.INFO)
    df_data = pd.DataFrame(np.arange(DATA_SIZE * COL_SIZE).reshape(DATA_SIZE, COL_SIZE),
                           columns=[f'a{i + 1}' for i in range(COL_SIZE)])
    ray.init(address=f"192.168.31.222:6000")
    df_data = ray.put(df_data)
    all_result=[]
    if ray.is_initialized():
        t1 = time.time()
        for i in range(ALL_ROUNDS):
            result_i = [func.remote(df_data, i + 1) for i in
                       range(NUM_KERNELS * i, min(NUM_KERNELS * (i + 1), NUM_TASK-1))]
            result_i = ray.get(result_i)
            all_result.extend(result_i)
        # print(f'Used time -> {time.time() - t1}  | result -> {result}')
        print(f'Used time -> {time.time() - t1} ')
