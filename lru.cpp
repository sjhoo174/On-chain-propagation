
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include <iostream>
#include <cmath>
#include<ctime>

using namespace std;
using namespace ROCKSDB_NAMESPACE;

using namespace std::chrono;

#define DEBUG 0
// #define DEBUG2 1
#define chronos 0
#define chronosTWO 0

class LRUCache {
    

    
  
public:
    // store keys of cache
    list<int> dq;
  
    // store references of key in cache
    unordered_map<int, double*> ma;
    
    int csize; // maximum capacity of cache
    int noOfColumns;    
    DB* db;
    int counter = 0;

    LRUCache(int,int, DB*);
    double* refer(int);
    void getFromDB(int, double*);
    void writeDB(int index, double* target);
    void write(int to_index, double *target);

};


  
// Declare the size
LRUCache::LRUCache(int size, int noOfColumns, DB* db)
{
    csize = size;
    this-> noOfColumns = noOfColumns;
    this -> db = db;
    ma.reserve(size*2);

    
}

  
// Refers key x with in the LRU cache
double* LRUCache::refer(int x)
{
    // not present in cache
    if (ma.find(x) == ma.end()) {
        // cache is full
        if (dq.size() == csize) {
            // delete least recently used element
            int last = dq.back();

            // Pops the last elmeent
            dq.pop_back();
            dq.push_front(x);   

            // writeDB(last, ma[last]);
            std::string v = "";
            for (int i=0; i <= noOfColumns-1; i++) {
                double valu = ma[last][i];
                valu = round(valu * 1000) / 1000;
                v =  v + to_string(valu) + ";";
            }


            WriteBatch batch;
            batch.Put("row_index_" + std::to_string(last), v);
            db->Write(WriteOptions(), &batch);
            
            // Erase the last
            ma.erase(last);


            double *temp = new double[noOfColumns];

            //getFromDB(x, temp);
            std::string value;
            this -> db->Get(ReadOptions(), "row_index_" + std::to_string(x), &value);
        
            double val =0;
            int pos = 0;
            int next = 0;

            for (int i=0; i <= noOfColumns -1; i++) {
                next = value.find(';');
                sscanf(value.substr(pos, next).c_str(), "%lf", &val);
                value = value.substr(next+1);
                temp[i] = val;
                pos = 0;
                
            }

            ma[x] = temp;
        
            return ma[x];
        }

        double *temp = new double[noOfColumns];
        //getFromDB(x, temp);
        std::string value;
        this -> db->Get(ReadOptions(), "row_index_" + std::to_string(x), &value);
    
        double val =0;
        int pos = 0;
        int next = 0;

        for (int i=0; i <= noOfColumns -1; i++) {
            next = value.find(';');
            sscanf(value.substr(pos, next).c_str(), "%lf", &val);
            value = value.substr(next+1);
            temp[i] = val;
            pos = 0;
        }
        ma[x] = temp; 
        dq.push_front(x);

        return ma[x];
        
    }
  
    // present in cache
    else {
  
        return ma[x];

        
    }

}

void LRUCache::write(int x, double* target) {

    // not present in cache
    if (ma.find(x) == ma.end()) {
        // cache is full
        if (dq.size() == csize) {
            // delete least recently used element

            int last = dq.back();
    
            // Pops the last elmeent
            dq.pop_back();
            dq.push_front(x);


            // writeDB(last, ma[last]);
            std::string value = ""; 
            for (int i=0; i <= noOfColumns-1; i++) {
                double val = ma[last][i];
                val = round(val * 1000) / 1000;
                value =  value + to_string(val) + ";";
            }

            WriteBatch batch;
            batch.Put("row_index_" + std::to_string(last), value);
            db->Write(WriteOptions(), &batch);

            double *temp = new double[noOfColumns];
            for (int i =0; i<noOfColumns;i++) {
                temp[i] = target[i];
            }
            ma.erase(last);
            ma[x] = temp;

            return;
        }

        double *temp = new double[noOfColumns];
        for (int i =0; i<noOfColumns;i++) {
            temp[i] = target[i];
        }
        dq.push_front(x);
        ma[x] = temp;

        return;

    }
  
    // present in cache
    else {
        double *temp = new double[noOfColumns];
        for (int i =0; i<noOfColumns;i++) {
            temp[i] = target[i];
        }
        ma.erase(x);
        ma[x] = temp;
  
        return;
    }
}

// void LRUCache::writeDB(int row_index, double *array) {


//     std::string value = "";
//     for (int i=0; i <= noOfColumns-1; i++) {
//         int integer = (int) array[i];
//         double val = array[i] - integer;
//         int rem = val * 1000;
//         std::string remainder= std::to_string(rem);
//         std::string intportion = std::to_string(integer);
//         value =  value + intportion + "." + remainder + ";";

//     }

        // WriteBatch batch;
//     batch.Put("row_index_" + std::to_string(row_index), value);
//     db->Write(WriteOptions(), &batch);

// }

// inline void LRUCache::getFromDB(int row_index, double* temp) {


//     std::string value;
//     this -> db->Get(ReadOptions(), "row_index_" + std::to_string(row_index), &value);
  
//     double val =0;
//     int pos = 0;
//     int next = 0;

//     for (int i=0; i <= noOfColumns -1; i++) {
//         next = value.find(';');
//         sscanf(value.substr(pos, next).c_str(), "%lf", &val);
//         value = value.substr(next+1);
//         temp[i] = val;
//         pos = 0;
//         
//     }

// }
  