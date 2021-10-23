import logging
import numpy as np
import pandas as pd
import dask.dataframe as dd
import time
from datetime import datetime
from google.cloud.bigquery.job import LoadJobConfig, QueryJob
from typing import List, Dict, Callable

from eth_graph.cache import CacheOnRocks, int_to_bytes, array_to_bytes, bytes_to_array
from eth_graph.gcp_tools import get_gcp_client, download_query_job
import eth_graph.eth_bq_sql as bq
from eth_graph.eth_utils import get_file_parition_count

import os
import ctypes
from ctypes import cdll

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Ethereum(object):
    def __init__(
        self,
        mapping: Dict[str, int],
        current_block: int = 0,
        cache_size: int = 1e6,
        address_db_path: str = "node.db",
        txn_db_path: str = "tx.db",
        gcs_bucket: str = "",
        bq_dataset_name: str = "",
    ) -> None:
        """
        Sets up initial Ethereum object for propagation
        Creates two types of google big query client upon load
        To make sure that it can query data please configure GOOGLE_APPLICATION_CREDENTIALS
          e.g. export GOOGLE_APPLICATION_CREDENTIALS="/home/kite/GCloud-3ac3585d4c8b.json"
        current_block is used to keep track of progress
        Address dataset should have a propagation_group column with the categories to propagate over
        """
        self.bq_client = get_gcp_client(client_type="bigquery")
        self.storage_client = get_gcp_client(client_type="storage")
        self.address_cache = CacheOnRocks(
            cache_size,
            address_db_path,
            key_to_bytes=int_to_bytes,
            value_to_bytes=array_to_bytes,
            bytes_to_value=bytes_to_array,
        )
        self.gcs_bucket = gcs_bucket
        self.start_block = None
        self.end_block = None
        self.current_block = current_block
        self.max_processed_txn_index = 0
        self.cache_size = cache_size

        self.mapping = mapping
        self.map_vec = self.map2vec(self.mapping)
        self.map_vec_keys = self.map_vec.keys()

        self.address_dtype = {
            "index": "int64",
            "propagation_group": "object",
            "key_node_flag": "bool",
        }
        self.txn_dtype = {
            "txn_index": "int64",
            "from_address": "int64",
            "to_address": "int64",
            "to_address_key_node_flag": "bool",
            "to_address_prev_balance": "object",
            "value": "object",
            "block_number": "int64",
            
        }
        
        # self.initCLib()
        
        

    # def initCLib(self):
    #     dir_path = os.path.dirname(os.path.realpath(__file__))
    #     self.lib = np.ctypeslib.load_library('process.so', dir_path)
    #     self.lib.process.restype=None
    #     self.lib.process.argtypes = [np.ctypeslib.ndpointer(float, ndim=1,
    #                                             flags='aligned'),
    #                             np.ctypeslib.ndpointer(float, ndim=1,
    #                                             flags='aligned'),
    #                             ctypes.c_double, ctypes.c_double, ctypes.c_int, ctypes.c_int]
                                
    def initLRU(self, cache_size, noOfColumns, dbPath):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.libLRU = np.ctypeslib.load_library('lru.so', dir_path)

        self.libLRU.updateAddrValue.restype=None
        self.libLRU.updateAddrValue.argtypes = [ctypes.c_int,np.ctypeslib.ndpointer(float, ndim=1,
                                                flags='aligned'), ctypes.c_int]

        self.libLRU.process.restype=None
        self.libLRU.process.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_double, np.ctypeslib.ndpointer(float, ndim=1,
                                                flags='aligned'), ctypes.c_int, ctypes.c_double,]


        self.libLRU.closeDB.restype=None
        self.libLRU.closeDB.argtypes = None

        self.libLRU.initLRU.restype=None
        self.libLRU.initLRU.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_char_p]

        self.libLRU.pullAddr.restype= None
        self.libLRU.pullAddr.argtypes=[np.ctypeslib.ndpointer(float, ndim=1), ctypes.c_int]

        self.libLRU.testput.restype= None
        self.libLRU.testput.argtypes=[np.ctypeslib.ndpointer(float, ndim=1), np.ctypeslib.ndpointer(float, ndim=1), ctypes.c_int]

        self.libLRU.initLRU(cache_size, noOfColumns, dbPath)

    def check_and_gen_client(self, client_type: str = "bigquery") -> None:
        if (client_type == "bigquery") & (self.bq_client is None):
            self.bq_client = get_gcp_client(client_type="bigquery")
        elif (client_type == "storage") & (self.storage_client is None):
            self.bq_client = get_gcp_client(client_type="storage")
        else:
            pass

    def map2vec(self, mapping: Dict[str, int]) -> Dict[str, np.array]:
        """
        Returns a dictionary which maps each category to a numpy array.
        The array has 1 in the position specified by the key of the map,
        and 0 in the other cells.
        """        
        map_vec = {}
        if mapping:
            self.noOfColumns = max(mapping.values()) + 1
            self.v_source = np.array([float(5.0),float(9.0)])
            self.v_target = np.array([float(5.0),float(9.0)])
            for k, v in mapping.items():
                map_vec[k] = np.zeros(self.noOfColumns, dtype = np.float64)
                map_vec[k][v] = 1
            logger.info(f"{self.noOfColumns} propagation groups found")
        else:
            logger.info("Continuing with no mapping provided")
        return map_vec

    def download_addresses(
        self,
        start_block: int,
        end_block: int,
        propagation_group: str = "category",
        path: str = None,
        token_name: str = "ETH",
        bq_dataset_name: str = "",
        callback: Callable[[str], None] = None,
    ) -> QueryJob:
        """Downloads addresses for specified start_block, end_block and ERC tokens"""
        self.noOfAddresses = end_block - start_block + 1
        self.initLRU(int(self.cache_size), int(self.noOfColumns), (path + "appended").encode('utf-8'))
        return self.download(
            start_block=start_block,
            end_block=end_block,
            file_name=f"new-addresses-blk-{start_block:012}-{end_block:012}",
            bq_function_name="get_addresses",
            path=path,
            token_name=token_name,
            bq_dataset_name=bq_dataset_name,
            callback=callback,
            propagation_group=propagation_group,
        )

    def download_txns(
        self,
        start_block: int,
        end_block: int,
        path: str = None,
        token_name: str = "ETH",
        bq_dataset_name: str = "",
        callback: Callable[[str], None] = None,
    ) -> QueryJob:
        """Downloads transactions for specified start_block, end_block and ERC tokens"""
        return self.download(
            start_block=start_block,
            end_block=end_block,
            file_name=f"txn-edges-blk-{start_block:012}-{end_block:012}",
            bq_function_name="get_transactions",
            path=path,
            token_name=token_name,
            bq_dataset_name=bq_dataset_name,
            callback=callback,
        )

    def download(
        self,
        start_block: int,
        end_block: int,
        file_name: str,
        bq_function_name: str,
        path: str = None,
        token_name: str = "ETH",
        bq_dataset_name: str = "",
        callback: Callable[[str], None] = None,
        **kwargs,
    ) -> QueryJob:
        """
        If no path is specified it returns the results into memory
        If a path is specified it writes the files asynchronously to gcp before importing to local
        machine
        Blocks from start_block to end_block - 1 are downloaded
        """
        self.check_and_gen_client()
        if path is None:
            return getattr(bq, bq_function_name)(
                self.bq_client,
                start_block,
                end_block,
                token_name=token_name,
                direct_download=True,
                bq_dataset_name=bq_dataset_name,
                kwargs=kwargs,
            )
        else:
            query_job = getattr(bq, bq_function_name)(
                self.bq_client,
                start_block,
                end_block,
                token_name=token_name,
                direct_download=False,
                bq_dataset_name=bq_dataset_name,
                kwargs=kwargs,
            )
            download_query_job(
                query_job,
                self.gcs_bucket,
                f"{bq_dataset_name}/propagation/input/{token_name}",
                path,
                file_name,
                bq_client=self.bq_client,
                storage_client=self.storage_client,
                callback=callback,
            )
            return query_job

    def process_addresses(
        self,
        start_block: int,
        end_block: int,
        lookup_col: str = "propagation_group",
        path: str = None,
        bq_dataset_name: str = "",
    ) -> None:
        if path is None:
            new_addresses = self.download_addresses(
                start_block, end_block, path, bq_dataset_name=bq_dataset_name
            )
        else:
            file_name = f"new-addresses-blk-{start_block:012}-{end_block:012}"
            file_path = f"{path}/{file_name}*.csv.gz"
            file_parition_count = get_file_parition_count(file_path)
            address_ddf = dd.read_csv(
                file_path, compression="gzip", dtype=self.address_dtype, blocksize=None,
            )
            if file_parition_count > 1:
                logger.info(
                    f"{file_name} file_parition_count greater than 1, merge and sort on"
                )
                address_ddf = address_ddf.set_index("index")
                address_ddf["index"] = address_ddf.index
            new_addresses = address_ddf.itertuples()
        updateFunc = self.libLRU.updateAddrValue

        # test array
        self.ty = np.array([float(5.0),float(9.0)])

        for addr in new_addresses:
            if (addr.index % 1000000 == 0) :
                print(addr.index)
            

            if getattr(addr, lookup_col) not in self.map_vec_keys:
                # NaN and other not specified groups will be mapped as 'Other'
                # updateFunc(addr.index, self.map_vec["Other"], self.noOfColumns)
                self.address_cache[addr.index] = self.map_vec["Other"]
                # self.libLRU.pullAddr(self.ty, addr.index)
                # if (abs(self.ty[0] - self.address_cache[addr.index][0]) > 0.01):
                #     print("C address")
                #     print(self.ty)
                #     print(self.address_cache[addr.index])
                # if (abs(self.ty[1] - self.address_cache[addr.index][1]) > 0.01):
                #     print("C address")
                #     print(self.ty)
                #     print(self.address_cache[addr.index])

            else:
                # updateFunc(addr.index, self.map_vec[getattr(addr, lookup_col)], self.noOfColumns)
                self.address_cache[addr.index] = self.map_vec[getattr(addr, lookup_col)]
                # self.libLRU.pullAddr(self.ty, addr.index)
                # if (abs(self.ty[0] - self.address_cache[addr.index][0]) > 0.01):
                #     print("C address")
                #     print(self.ty)
                #     print(self.address_cache[addr.index])
                # if (abs(self.ty[1] - self.address_cache[addr.index][1]) > 0.01):
                #     print("C address")
                #     print(self.ty)
                #     print(self.address_cache[addr.index])


    def testPut(self, v, t, i):
        self.libLRU.testput(v, t, i)
        print(t)
    
    def testPull(self):
        self.libLRU.test(self.v_target)
        print(self.v_target)

    def propagate_txns(
        self,
        start_block,
        end_block,
        path=None,
        token_name="ETH",
        bq_dataset_name="",
    ) -> List[Dict[int, np.array]]:
        if path is None:
            new_txns = self.download_txns(
                start_block,
                end_block,
                path,
                token_name=token_name,
                bq_dataset_name=bq_dataset_name,
            )
        else:
            file_name = f"txn-edges-blk-{start_block:012}-{end_block:012}"
            file_path = f"{path}/{file_name}*.csv.gz"
            file_parition_count = get_file_parition_count(file_path)
          
            txn_ddf = dd.read_csv(
                file_path, compression="gzip", dtype=self.txn_dtype, blocksize=None,
            )
            if file_parition_count > 1:
                logger.info(
                    f"{file_name} file_parition_count greater than 1, merge and sort on"
                )
                txn_ddf = txn_ddf.set_index("txn_index")
                txn_ddf["txn_index"] = txn_ddf.index
            new_txns = txn_ddf.itertuples()
        transaction_scores = []
        self.yo = True

        for row in new_txns:
            if row.txn_index < self.max_processed_txn_index:
                raise Exception(
                    f"txn_index order incorrect, row txn_index {row.txn_index} can't be less than max_processed_txn_index{self.max_processed_txn_index}"
                )
            transaction_scores.append(
                {
                    "name": token_name,
                    "txn_index": row.txn_index,
                    "score_vector": self._process_row(row).tolist(),
                }
            )
            self.max_processed_txn_index = row.txn_index
        self.current_block = end_block - 1
        return transaction_scores

    def close(self):
        self.libLRU.closeDB()

    # C implementation
    # def _process_row(self, row) -> np.ndarray:

    #     self.libLRU.pullAddr(self.v_source, row.from_index)
       
    #     if row.to_address_key_node_flag == False:
    #         self.libLRU.process(int(row.from_index), int(row.to_index), float(row.to_address_prev_balance), self.v_target, self.noOfColumns, float(row.value))
    #     return self.v_source

    # Original Python implementation
    def _process_row(self, row) -> np.ndarray:
    #     self.libLRU.pullAddr(self.v_source, row.from_index)
    #     temp = np.array([float(9.0), float(6.0)])
    #     self.libLRU.pullAddr(temp, row.to_index)

    #     if row.to_address_key_node_flag == False:
    #         self.libLRU.process(int(row.from_index), int(row.to_index), float(row.to_address_prev_balance), self.v_target, self.noOfColumns, float(row.value))

        source = self.address_cache[row.from_index]
        target = self.address_cache[row.to_index]
    #     # If not sending to key node then propagate, else keep initial assignment of v (do nothing)
    #     # Convert array to float before saving to avoid precision errors between python object and
    #     # float64
   
        if row.to_address_key_node_flag == False:
            new_bal_arr = (target * np.float(row.to_address_prev_balance)) + (
                source * np.float(row.value)
            )
            temp_prop_arr = new_bal_arr / new_bal_arr.sum()
            # Floor the indices of array, whose source of fund proportion falls below 0.02
            # Then redistribute the total value floored to the remaining indices
            floored_arr = np.array([0 if x < 0.02 else x for x in temp_prop_arr])
            new_prop_arr = floored_arr / sum(floored_arr)
            self.address_cache[row.to_index] = new_prop_arr.astype("float64")
            

    #         if self.yo:
           
    #             if (abs(self.v_target[0] - self.address_cache[row.to_index][0]) > 0.01):
    #                 print("C code: ")
    #                 print("temp=", temp)
    #                 print("selfvtarget=", self.v_target)
    #                 print("selfvsource=", self.v_source)     
    #                 print("Python: ")
    #                 print(self.address_cache[row.to_index])
    #                 print(target)
    #                 print(source)
    #                 print(int(row.to_index))
    #                 print(int(row.from_index))
    #                 print("PREV=",row.to_address_prev_balance)
    #                 print("VALUE=",row.value)
                    
    #                 self.yo = False
    #             if (abs(self.v_target[1] - self.address_cache[row.to_index][1]) > 0.01):
    #                 print("C code: ")
    #                 print("temp=", temp)

    #                 print("selfvtarget=", self.v_target)
    #                 print("selfvsource=", self.v_source) 
                
    #                 print("Python: ")
    #                 print(self.address_cache[row.to_index])
    #                 print(target)
    #                 print(source)
    #                 print(int(row.to_index))
    #                 print(int(row.from_index))
    #                 print("PREV=",row.to_address_prev_balance)
    #                 print("VALUE=",row.value)

    #                 self.yo = False
                

        return source
