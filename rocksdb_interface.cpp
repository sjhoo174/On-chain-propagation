#include <string>
#include <cstdio>
#include <cmath>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "lru.cpp"

#define DEBUG 0

// g++ -fno-rtti rocksdb_interface.cpp -shared -olru.so ../rocksdb/librocksdb.so -I../rocksdb/include -O2 -std=c++11 -lpthread -lrt -ldl -std=c++11  -faligned-new -DHAVE_ALIGNED_NEW -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX -DROCKSDB_BACKTRACE -DROCKSDB_R^CGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -DROCKSDB_AUXV_GETAUXVAL_PRESENT -march=native   -DHAVE_SSE42  -DHAVE_PCLMUL  -DHAVE_AVX2  -DHAVE_BMI  -DHAVE_LZCNT -DHAVE_UINT128_EXTENSION -DROCKSDB_SUPPORT_THREAD_LOCAL -ldl -lpthread -fPIC

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "";
DB* db;
LRUCache* lru;
WriteBatch batch;


// #ifndef chronosTWO
// #define chronosTWO 1
// #endif

extern "C" {

    void pullAddr(double*, int);

    void updateAddrValue(int row_index, double* array, int noOfColumns) {

        lru -> write(row_index, array);
        
    }

    void initDB() {
    
        Options options;
        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        // create the DB if it's not already present
        options.max_open_files = 300000;
        options.write_buffer_size = 67108864;
        options.create_if_missing = true;
        options.max_write_buffer_number = 3;

        // open DB
        Status s = DB::Open(options, kDBPath, &db);
        assert(s.ok());
            
    }

    void pullAddr(double*, int);

    void process(int from_index, int to_index, double prev_balance, double* retarget, int noOfColumns, double value) {

        double *source = lru ->refer(from_index);
        double *target = lru -> refer(to_index);
        
        double sum =0.0;
        for (int j=0; j< noOfColumns; j++) {
            retarget[j] = target[j]*prev_balance + source[j]*value;
            
        }

        for (int k=0; k< noOfColumns; k++) {
            sum += retarget[k];
            
        }
      

    // flooring
        for (int z=0; z< noOfColumns; z++) {
            retarget[z] = retarget[z] / sum;
            if (retarget[z] < 0.02) {
                retarget[z] = 0;
            }
            
        }   
      
        sum = 0;
        for (int a=0; a< noOfColumns; a++) {
            sum += retarget[a];
            
        }   
        
        for (int y=0; y< noOfColumns; y++) {
            retarget[y] = retarget[y] / sum;
            
        }   

        sum = 0;
        for (int a=0; a< noOfColumns; a++) {
            sum += retarget[a];
            
        }   
        
        lru -> write(to_index, retarget);
   
    }

    void initLRU(int cacheSize, int noOfColumns, const char* dbPath) {
        kDBPath = dbPath;
        initDB();
        lru = new LRUCache(cacheSize, noOfColumns, db);
    }

    void closeDB() {
        delete db;
        delete lru;
    }

    void pullAddr(double* array, int row) {
        for (int i = 0;i< 2;i++) {
            array[i] = lru ->refer(row)[i];
        }
    }

    void testput(double *array, double *temp, int row_index) {
        std::string value = "";
        for (int i=0; i <= 1; i++) {
            int integer = (int) array[i];
            double val = array[i] - integer;
            int rem = val * 1000;
            std::string remainder= std::to_string(rem);
            std::string intportion = std::to_string(integer);
            value =  value + intportion + "." + remainder + ";";
            
        }   

        batch.Put("row_index_" + std::to_string(row_index), value);
        db->Write(WriteOptions(), &batch);

        batch.Clear();

        std::string w;
        db->Get(ReadOptions(), "row_index_" + std::to_string(row_index), &w);

        for (std::string::size_type z = 0; z < w.size(); z++) {
            std::cout << w[z] << ' ';
            fflush(stdout);
        }

        double val = 0;
        std::string accumulate = "";
        int pos = 0;
        int next = 0;


        for (int j=0; j <= 1; j++) {
            next = w.find(';');
            accumulate = w.substr(pos, next);
            pos = next+1;
            sscanf(accumulate.c_str(), "%lf", &val);
            temp[j] = val;
            val = 0;
         }

    }

    

}

