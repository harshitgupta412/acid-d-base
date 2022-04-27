
#include <stdio.h>
#include <string.h>
#include "table.h"

#define SLOT_OFFSET 2
#define HEADER_SIZE_OFFSET 2
#define SIZE_OFFSET 2

#define END_OF_PAGE PF_PAGE_SIZE

// get number of slots in the page buffer
int  getNumSlots(byte *pageBuf)
{
    return DecodeShort(pageBuf);
}

// get offset of the Nth slot, where N is 0 indexed
int  getNthSlotOffset(int slot, char* pageBuf)
{
    assert(slot < getNumSlots(pageBuf));
    return DecodeShort(pageBuf + HEADER_SIZE_OFFSET + slot * SLOT_OFFSET);
}

// get length of the `slot` record
int  getLen(int slot, byte *pageBuf)
{
    int offset = getNthSlotOffset(slot, pageBuf);
    if(offset == -1) return -1;
    return DecodeShort(pageBuf + offset);
}

void Table::deleteRow(int rowId){
    Table_Delete(table, rowId);
    char record[MAX_PAGE_SIZE];
    int num_bytes = Table_Get(table, rowId, record, MAX_PAGE_SIZE);
    if(num_bytes == -1) {
        return;
    }

    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();   
            AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &sch), rowId);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();
            AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &sch), rowId);
        }
    }
}

Table::Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes): schema(*_schema) {
    name += db_name;
    name += ".";
    name += table_name;
    name += ".tbl";
    this->db_name = db_name;
    if(Table_Open((char*)name.c_str(), (Schema_*)_schema->getSchema(), overwrite, &table)== -1){
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

const Schema& Table::getSchema() {
    return schema;
}

bool Table::addRow(void* data[], bool update) {
    byte** pk_value = new byte*[pk_size];
    for(int i=0; i<pk_size; i++) {
        pk_value[i] = (byte*)data[pk_index[i]];
    }
    int rid = Table_Search(table, pk_index, pk_value, pk_size);
    if(rid != -1 && !update) return false;
    else if(rid != -1) deleteRow(rid);
    char record[MAX_PAGE_SIZE];
    Schema_ sch = *schema.getSchema();

    int len = encode(&sch, (char**)data, record, MAX_PAGE_SIZE);
    Table_Insert(table, record, len, &rid);
    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &sch), rid);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLength, getNthfield(record, indexes[i].indexNo, &sch), rid);
        }
    }
    return true;
}

std::string Table::get_name(){
    return this->name;
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

bool Table::deleteRow(void** pk) {
    int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
    if(rid == -1) return true;
    deleteRow(rid);
    return true;
}

void** Table::getRow(void** pk) {
    int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
    if(rid == -1) return NULL;
    char record[MAX_PAGE_SIZE];
    int len = Table_Get(table, rid, record, MAX_PAGE_SIZE);
    void** data = new void*[schema.getSchema()->numColumns];
    Schema_ sch = *schema.getSchema();
    decode(&sch, (char**)data, record, len);
    return data;
}

void Table::print() {
    Schema_ sch = *schema.getSchema();   
    Table_Scan(table, &sch, print_row);
}

Table* Table::query(bool (*callback)(RecId, byte*, int)){
    Table *t = new Table(&schema, (char*)"result", (char*)this->db_name.c_str(), false, indexes);
    int pageNo, err;
    char *pageBuf;
    Schema_ sch = *schema.getSchema();

    if ( (err = PF_GetFirstPage(table->fileDesc, &pageNo, &pageBuf)) == PFE_EOF)
        return t;
    else if ( err >= 0 || err == PFE_PAGEFIXED )
    {
        do
        {
            int nslots = getNumSlots(pageBuf);
            for (int i = 0; i < nslots; i++)
            {
                int len = getLen(i, pageBuf);
                if(len == -1) continue;
                bool result = callback((pageNo << 16) | i, pageBuf + getNthSlotOffset(i, pageBuf)+2, len);
                if (result){
                    void** data = new void*[sch.numColumns];
                    decode(&sch, (char**)data, pageBuf + getNthSlotOffset(i, pageBuf)+2, len);
                    t->addRow(data, false);
                }
            }
            // if (err != PFE_PAGEFIXED) checkerr(PF_UnfixPage(table->fileDesc, pageNo, 0), "Table_ scan : unfix page");
            err = PF_GetNextPage(table->fileDesc, &pageNo, &pageBuf);
        } while( err >= 0 || err == PFE_PAGEFIXED );
        assert(err == PFE_EOF);
    }
        // else
        //     checkerr(err, "Table_ scan : get first page");

    return t;
}

std::vector<char*> Table::getPrimaryKey() {
    std::vector<char*> pk;
    for(int i=0; i<pk_size; i++) {
        pk.push_back(schema.getSchema()->columns[pk_index[i]].name);
    }
    return pk;
}

int Table::createIndex(int col) {
    for(int i=0; i<indexes.size(); i++) {
        if(indexes[i].indexNo == col) return -1;
    }

    IndexData index;
    index.indexNo = col;
    switch(schema.getSchema()->columns[col].type) {
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

bool Table::eraseIndex(int id) {
    for(int i=0; i<indexes.size(); i++) {
        if(indexes[i].indexNo == id) {
            assert(AM_DestroyIndex((char*)name.c_str(), id) == AME_OK);
            indexes.erase(indexes.begin() + i);
            return true;
        }
    }
    return false;
}

bool Table::close() {
    Table_Close(table);
    for(int i=0; i<indexes.size(); i++) {
        if(indexes[i].isOpen) {
            assert(PF_CloseFile(indexes[i].fileDesc) == PFE_OK);
            indexes[i].isOpen = false;
        }
    }
    table = NULL;
    return true;
}

Table::~Table() {
    if(table != NULL)
        close();
    free (pk_index);
    free (table);
}

std::string Table::encodeTable() {
    std::string str;
    char* r = new char[name.size() + 2];
    int len = EncodeCString((char*)name.c_str(), r, name.size()+2);
    str.append(r,len);
    free(r);
    str += schema.encodeSchema();
    r = (char*) malloc(sizeof(int));
    EncodeInt(indexes.size(), r);
    str.append(r,4);
    free(r);
    for(int i=0; i<indexes.size(); i++) {
        r = (char*) malloc(sizeof(int));
        EncodeInt(indexes[i].indexNo, r);
        str.append(r,4);
        free(r);
        r = (char*) malloc(sizeof(char));
        EncodeInt(indexes[i].attrType, r);
        str.append(r,4);
        free(r);
        r = (char*) malloc(sizeof(int));
        EncodeInt(indexes[i].attrLength, r);
        str.append(r,4);
        free(r);
    }
    return str;
}


Table decodeTable(byte* s, int max_len ) {
    char* r;
    int len = DecodeCString2(s, &r, max_len);
    std::string name(r,len);
    free(r);
    s += len;
    
    Schema schema = decodeSchema(s, max_len, &len);
    s+=len;
    int num_indexes = DecodeInt(s);
    s += sizeof(int);
    std::vector<IndexData> indexes(num_indexes);
    for(int i=0; i<num_indexes; i++) {
        indexes[i].indexNo = DecodeInt(s);
        s += sizeof(int);
        indexes[i].attrType = DecodeInt(s);
        s += sizeof(int);
        indexes[i].attrLength= DecodeInt(s);
        s += sizeof(int);
    }
    return Table(&schema,(char*)name.substr(name.find('.'),name.size()).c_str() , (char*)name.substr(0, name.find('.')).c_str(), false, indexes);
}
