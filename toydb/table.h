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
#include <algorithm>
#include <assert.h>

#define MAX_PAGE_SIZE 4000
#define MAX_VARCHAR_LENGTH 50
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

    std::vector<std::pair<char, std::string> > table_logs;

    void deleteRow(int rowId, bool log = false);
    

    public:

    bool addRowFromByte(byte* record, int len, bool update);
    void getRecordAsBytes(void** pk, char**record, int* len);
    std::vector<std::pair<int, void**> > get_records();
    std::string get_name();
    std::string get_db_name();
    std::string get_table_name();

    Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes, bool index_pk = true);

    Table(Table* t2);

    const Schema& getSchema();
    Table_* getTable(); 

    bool addRow(void* data[], bool update, bool log = false);

    bool deleteRow(void** pk, bool log = false); 
    
    void** getRow(void** pk);

    void print();

    std::vector<char*> getPrimaryKey(); //


    int createIndex(std::vector<int> col); //

    bool eraseIndex(int id); //

    Table* query(bool (*callback)(RecId, byte*, int)); //

    Table* queryIndex(int indexNo, int op, std::vector<void*> values); //

    bool close();

    ~Table();

    std::string encodeTable(); //

    void rollback();
    void clear_logs();

    Table* project(std::vector<int> cols);      
};

Table decodeTable(byte* s, int max_len ); //
Table* table_union(Table* t1, Table* t2);
Table* table_intersect(Table* t1, Table* t2);
Table* table_join(Table* t1, Table* t2, std::vector<int> &cols1, std::vector<int> &cols2);

#endif
