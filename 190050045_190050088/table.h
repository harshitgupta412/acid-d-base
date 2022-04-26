// TODO: Check foreign key constraints on addition.

#ifndef _TABLE_H_
#define _TABLE_H_

#include "dblayer/tbl.h"
#include "amlayer/am.h"
#include "pflayer/pf.h"
#include "schema.h"
#include <string>
#include <iostream>
#include <vector>
#include <assert.h>

#define MAX_PAGE_SIZE 4000
#define MAX_VARCHAR_LENGTH 100
struct IndexData {
    int indexNo;
    char attrType;
    int attrLength;
    bool isOpen = false;
    int fileDesc;
};


class Table {
    Table_ *table;
    std::string name;
    Schema schema;
    int* pk_index;
    int pk_size;
    std::vector<IndexData> indexes;

    void deleteRow(int rowId){
        Table_Delete(table, rowId);
        char record[MAX_PAGE_SIZE];
        int num_bytes = Table_Get(table, rowId, record, MAX_PAGE_SIZE);
        if(num_bytes == -1) {
            return;
        }

        for(int i=0; i<indexes.size(); i++){
            if(indexes[i].isOpen){
                AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &schema.getSchema()), rowId);
            }
            else {
                indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
                indexes[i].isOpen = true;
                AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &schema.getSchema()), rowId);
            }
        }
    }

    public:

    Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes): schema(*_schema) {
        name += db_name + ".";
        name += table_name;
        name += ".tbl";
        if(Table_Open((char*)name.c_str(), &_schema->getSchema(), overwrite, &table)== -1){
            std::cout << "Error opening table " << name << std::endl;
            exit(1);
        }
        std::vector<int> _pk = _schema->getpk();
        pk_index = new int[_pk.size()];
        for(int i=0; i<_pk.size(); i++) {
            pk_index[i] = _pk[i];
        }
        pk_size = _pk.size();
        indexes = _indexes;
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
        else if(rid != -1) deleteRow(rid);
        char record[MAX_PAGE_SIZE];
        int len = encode(&schema.getSchema(), (char**)data, record, MAX_PAGE_SIZE);
        Table_Insert(table, record, len, &rid);
        for(int i=0; i<indexes.size(); i++){
            if(indexes[i].isOpen){
                AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &schema.getSchema()), rid);
            }
            else {
                indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
                indexes[i].isOpen = true;
                AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &schema.getSchema()), rid);
            }
        }
        return true;
    }

    bool deleteRow(void** pk) {
        int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
        if(rid == -1) return true;
        deleteRow(rid);
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

    std::vector<char*> getPrimaryKey() {
        std::vector<char*> pk;
        for(int i=0; i<pk_size; i++) {
            pk.push_back(schema.getSchema().columns[pk_index[i]].name);
        }
        return pk;
    }

    int createIndex(int col) {
        for(int i=0; i<indexes.size(); i++) {
            if(indexes[i].indexNo == col) return -1;
        }

        IndexData index;
        index.indexNo = col;
        switch(schema.getSchema().columns[col].type) {
            case VARCHAR:
                index.attrType = 'c';
                index.attrLength = MAX_VARCHAR_LENGTH;
                break;
            case INT:
                index.attrType = 'i';
                index.attrLength = 4;
                break;
            case FLOAT:
                index.attrType = 'f';
                index.attrLength = 4;
                break;
        }

        assert(AM_CreateIndex((char*)name.c_str(), index.indexNo, index.attrType, index.attrLength) == AME_OK);
        return col;
    }

    bool eraseIndex(int id) {
        for(int i=0; i<indexes.size(); i++) {
            if(indexes[i].indexNo == id) {
                assert(AM_DestroyIndex((char*)name.c_str(), id) == AME_OK);
                indexes.erase(indexes.begin() + i);
                return true;
            }
        }
        return false;
    }

    bool close() {
        Table_Close(table);
        for(int i=0; i<indexes.size(); i++) {
            if(indexes[i].isOpen) {
                assert(PF_CloseFile(indexes[i].fileDesc) == PFE_OK);
                indexes[i].isOpen = false;
            }
        }
        return true;
    }

    ~Table() {
        close();
        free (pk_index);
        free (table);
    }

    char* encodeTable() {
        std::string str;
        char* r = new char[name.size() + 2];
        EncodeCString((char*)name.c_str(), r, name.size()+2);
        str += r;
        free(r);
        str += schema.encodeSchema();
        char* r = (char*) malloc(sizeof(int));
        EncodeInt(indexes.size(), r);
        str += r;
        free(r);
        for(int i=0; i<indexes.size(); i++) {
            r = (char*) malloc(sizeof(int));
            EncodeInt(indexes[i].indexNo, r);
            str += r;
            free(r);
            r = (char*) malloc(sizeof(char));
            EncodeInt(indexes[i].attrType, r);
            str += r;
            free(r);
            r = (char*) malloc(sizeof(int));
            EncodeInt(indexes[i].attrLength, r);
            str += r;
            free(r);
        }
    }
};

Table decodeTable(byte* s, int max_len ) {
    std::string name;
    char* r;
    int len = DecodeCString2(s, &r, max_len);
    name = r;
    free(r);
    s += len;
    Schema schema = decodeSchema(s, max_len);
    int num_indexes = DecodeInt(s);
    s += sizeof(int);
    std::vector<IndexData> indexes(num_indexes);
    for(int i=0; i<num_indexes; i++) {
        indexes[i].indexNo = DecodeInt(s);
        s += sizeof(int);
        indexes[i].attrType = DecodeInt(s);
        s += sizeof(int);
        indexes[i].attrLength=DecodeInt(s);
        s += sizeof(int);
    }
    return Table(&schema,(char*)name.substr(name.find('.'),name.size()).c_str() , (char*)name.substr(0, name.find('.')).c_str(), false, indexes);
}

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
