import os
import glob

import numpy as np
import logging
import argparse
import orjson
import threading
from pathlib import Path
from typing import List
from datetime import datetime
from eth_graph.ethereum import Ethereum
from eth_graph.gcp_tools import get_gcp_client, upload_gcp_storage, gcs_to_bq
from eth_graph.eth_utils import clear_gcs_data, clear_local_data
from eth_graph.propagate import write_propagation_output_chunk
from eth_graph.import_data import import_data
from google.cloud import bigquery

import dask.dataframe as dd
import pandas as pd


# Initialize Ethereum object grouping to be propagated
eth=Ethereum(mapping = {'Mining':0 ,'Other':1}, current_block=0, cache_size=1e6, 
             address_db_path=os.path.join('eth_graph/data', "node.db"),gcs_bucket = "")

for thread in threading.enumerate(): 
    if (thread.name != 'MainThread'):
        thread.join()

# Download all address
eth.download_addresses(
    0,
    5000000,
    propagation_group="entity",
    path=os.path.join('eth_graph/data', "nodes"),
    bq_dataset_name="",
)   

for thread in threading.enumerate(): 
    if (thread.name != 'MainThread'):
        thread.join()

# Download transactions starting from the block where the hack first took place
start_block = 0        
end_block = 5000000
step = 50000
for i in range(start_block, end_block, step):
    end_of_interval = min(i + step, end_block )
    eth.download_txns(
        i,
        end_of_interval,
        path=os.path.join('eth_graph/data', "edges"),
        bq_dataset_name="",
    )

for thread in threading.enumerate(): 
    if (thread.name != 'MainThread'):

        thread.join()

import time

start = time.time()

eth.process_addresses(
    0,
    5000000,
    path=os.path.join('eth_graph/data', "nodes"),
    bq_dataset_name="",
)

# t = np.array([float(5.0),float(9.0)])
# g = np.array([float(3.0), float(2.0)])
# eth.testPut(t,g,5)

end = time.time()
difference = end - start
print(difference)


import time
start = time.time()
writting_jobs = []
start_block = 0
end_block =5000000
step = 50000
for i in range(start_block, end_block, step):
    end_of_interval = min(i + step, end_block)
    transaction_score_vectors = eth.propagate_txns(
        i,
        end_of_interval,
        path=os.path.join('eth_graph/data', "edges"),                                           
        bq_dataset_name="",
    )
    
    writer = threading.Thread(
        target=write_propagation_output_chunk,
        name="transaction_scores_writter_{}".format(i),
        kwargs={
            "output_path": "output/reg_prop/reg_prop_{}_{}.json".format(i, end_of_interval),    #Change accordingly on run
            "transaction_scores": [
                orjson.dumps(tsv) for tsv in transaction_score_vectors
            ],
        },
    )
    writting_jobs.append(writer)
    writer.start()
    

# Wait for writes to complete
[t.join() for t in writting_jobs]
end = time.time()
difference = end - start
print(difference)



eth.close()

