// TODO: Check foreign key constraints on addition.

#ifndef _TABLE_H_
#define _TABLE_H_


#ifdef __cplusplus /* If this is a C++ compiler, use C linkage */
extern "C" {
#endif

#include "dblayer/tbl.h"
#include "amlayer/am.h"
#include "pflayer/pf.h"

#ifdef __cplusplus /* If this is a C++ compiler, end C linkage */
}
#endif
#include "schema.h"
#include <string>
#include <iostream>
#include <vector>
#include <assert.h>

#define MAX_PAGE_SIZE 4000
#define MAX_VARCHAR_LENGTH 100
struct IndexData {
    int indexNo;
    int numCols;
    char* attrType;
    int* attrLen;
    bool isOpen = false;
    int fileDesc;
};


class Table {
    Table_ *table;
    std::string name;
    std::string db_name;
    Schema schema;
    int* pk_index;
    int pk_size;
    std::vector<IndexData> indexes;

    void deleteRow(int rowId);

    public:

    std::string get_name();

    Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes);

    const Schema& getSchema();

    bool addRow(void* data[], bool update);

    bool deleteRow(void** pk); //
    
    void** getRow(void** pk);

    void print();

    std::vector<char*> getPrimaryKey(); //

    int createIndex(std::vector<int> col); //

    bool eraseIndex(int id); //

    Table* query(bool (*callback)(RecId, byte*, int)); //

    bool close();

    ~Table();

    std::string encodeTable(); //
};

Table decodeTable(byte* s, int max_len ); //

#endif
