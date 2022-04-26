#ifndef _TABLE_H_
#define _TABLE_H_

#include "schema.h"
#include "dblayer/tbl.h"
#include <string>
#include <iostream>
#include <vector>
#define MAX_PAGE_SIZE 4000

class Table {
    Table_ *table;
    Schema schema;
    int* pk_index;
    int pk_size;
    public:

    Table(Schema* _schema, char* table_name, char* db_name, bool overwrite): schema(*_schema) {
        std::string name = "";
        name += db_name;
        name += table_name;
        name += ".tbl";
        Table_Open((char*)name.c_str(), &_schema->getSchema(), overwrite, &table);
        std::vector<int> _pk = _schema->getpk();
        pk_index = new int[_pk.size()];
        for(int i=0; i<_pk.size(); i++) {
            pk_index[i] = _pk[i];
        }
        pk_size = _pk.size();
    }

    const Schema& getSchema() {
        return schema;
    }

    bool addRow(void* data[], bool update) {
        byte** pk_value = new byte*[pk_size];
        for(int i=0; i<pk_size; i++) {
            pk_value[i] = (byte*)data[pk_index[i]];
        }
        int rid = Table_Search(table, pk_index, pk_value, pk_size);

        if(rid != -1 && !update) return false;
        else if(rid != -1) Table_Delete(table, rid);
        char record[MAX_PAGE_SIZE];
        int len = encode(&schema.getSchema(), (char**)data, record, MAX_PAGE_SIZE);
        Table_Insert(table, record, len, NULL);
        return true;
    }

    bool deleteRow(void** pk) {
        int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
        if(rid == -1) return true;
        Table_Delete(table, rid);
        return true;
    }
    
    void** getRow(void* pk) {
        int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
        if(rid == -1) return NULL;
        char record[MAX_PAGE_SIZE];
        int len = Table_Get(table, rid, record, MAX_PAGE_SIZE);
        void** data = new void*[schema.getSchema().numColumns];
        decode(&schema.getSchema(), (char**)data, record, len);
        return data;
    }

    void print() {
        Table_Scan(table, &schema.getSchema(), print_row);
    }

    
};

void print_row(void* callbackObj, int rid, byte* row, int len) {
    Schema_ *schema = (Schema_ *) callbackObj;
    void** data = new void*[schema->numColumns];
    decode(schema, (char**)data, row, len);
    for(int i=0; i<schema->numColumns; i++) {
        std::cout << data[i] << " ";
    }
    std::cout << std::endl;
}
#endif
