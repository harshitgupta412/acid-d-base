
#include <stdio.h>
#include <string.h>
#include "table.h"

#define SLOT_OFFSET 2
#define HEADER_SIZE_OFFSET 2
#define SIZE_OFFSET 2

#define END_OF_PAGE PF_PAGE_SIZE

// get unique index number from cols of table
int cols_to_indexNo(std::vector<int> cols, int maxcols)
{
    int indexNo = 0;
    for (int i = cols.size() - 1; i >= 0; i--)
    {
        indexNo = indexNo * maxcols + cols[i];
    }
    return indexNo;
}

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
    char record[MAX_PAGE_SIZE];
    int num_bytes = Table_Get(table, rowId, record, MAX_PAGE_SIZE);
    Table_Delete(table, rowId);
    if(num_bytes == -1) {
        return;
    }

    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();   
            AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(record, indexes[i].indexNo, &sch), rowId);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();
            AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(record, indexes[i].indexNo, &sch), rowId);
        }
    }
}

Table::Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes = std::vector<IndexData>(), bool index_pk = true): schema(*_schema) {
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

    if (index_pk)
    {
        int pkindexNo = cols_to_indexNo(_pk, _schema->getSchema()->numColumns);
        bool addDefaultIndex = true;
        for (int i = 0; i < indexes.size(); ++i)
            if (indexes[i].indexNo == pkindexNo)
                addDefaultIndex = false;
        if (addDefaultIndex)
        {
            createIndex(_pk);
        }
    }
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
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(record, indexes[i].indexNo, &sch), rid);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(record, indexes[i].indexNo, &sch), rid);
        }
    }
    return true;
}

bool Table::addRowFromByte(byte *data, int len, bool update) {
    byte** pk_value = new byte*[pk_size];
    for(int i=0; i<pk_size; i++) {
        pk_value[i] = (byte*)data[pk_index[i]];
    }
    int rid = Table_Search(table, pk_index, pk_value, pk_size);
    if(rid != -1 && !update) return false;
    else if(rid != -1) deleteRow(rid);
    Schema_ sch = *schema.getSchema();

    Table_Insert(table, data, len, &rid);
    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(data, indexes[i].indexNo, &sch), rid);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(name + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();
            AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, getNthfield(data, indexes[i].indexNo, &sch), rid);
        }
    }
    return true;
}

std::string Table::get_name(){
    return this->name;
}
void print_row(void* callbackObj, int rid, byte* row, int len) {
    Schema_ *schema = (Schema_ *) callbackObj;
    char* data[schema->numColumns];
    decode(schema, (char**)data, row+2, len);
    for(int i=0; i<schema->numColumns; i++) {
        switch(schema->columns[i].type) {
            case INT:
                printf("%d\t", DecodeInt(data[i]));
                break;
            case FLOAT:
                printf("%f\t", DecodeFloat(data[i]));
                break;
            case LONG:
                printf("%lld\t", DecodeLong(data[i]));
                break;
            case VARCHAR:
                printf("%s\t", (char*)data[i]);
                break;
            default:
                printf("%s\t", (char*)data[i]);
                break;
        }
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
    decode(&sch, (char**)data, record+2, len);
    return data;
}

void Table::print() {
    Schema_ sch = *schema.getSchema(); 
    schema.print();
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
                    decode(&    sch, (char**)data, pageBuf + getNthSlotOffset(i, pageBuf)+2, len);
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

int Table::createIndex(std::vector<int> cols) {
    int indexNo = cols_to_indexNo(cols, schema.getSchema()->numColumns);
    for(int i=0; i<indexes.size(); i++) {
        if(indexes[i].indexNo == indexNo) return -1;
    }

    IndexData index;
    index.indexNo = indexNo;
    index.numCols = cols.size();
    index.attrType = new char[cols.size()];
    index.attrLen = new int[cols.size()];
    for (int i = 0; i < cols.size(); i++) {
        switch(schema.getSchema()->columns[cols[i]].type) {
            case VARCHAR:
                index.attrType[i] = 'c';
                index.attrLen[i] = MAX_VARCHAR_LENGTH;
                break;
            case INT:
                index.attrType[i] = 'i';
                index.attrLen[i] = 4;
                break;
            case FLOAT:
                index.attrType[i] = 'f';
                index.attrLen[i] = 4;
                break;
            case LONG:
                index.attrType[i] = 'l';
                index.attrLen[i] = 8;
                break;
            default:
                fprintf(stderr, "invalid type : create index");
                exit(1);
        }
    }
    assert(AM_CreateIndex((char*)name.c_str(), index.indexNo, index.attrType, index.attrLen, index.numCols) == AME_OK);
    indexes.push_back(index);
    return indexNo;
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

Table* Table::queryIndex(int indexNo, int op, std::vector<void*> values)
{
    Table *t = new Table(&schema, (char*)"result", (char*)this->db_name.c_str(), false, std::vector<IndexData>(), false);
    int pageNo, err;
    char *pageBuf;
    Schema_ sch = *schema.getSchema();

    IndexData index;
    bool index_present = false;
    for (auto idx : indexes) {
        if (idx.indexNo == indexNo) {
            index_present = true;
            index = idx;
            break;
        }
    }

    if (!index_present) {
        fprintf(stderr, "Index not found in query index\n");
        exit(1);
    }

    if (index.numCols != values.size()) {
        fprintf(stderr, "Invalid number of values in query index\n");
        exit(1);
    }

    byte key[256];
    byte* keyPtr = key;
    int remaining_len = 256;
    for ( int i = 0; i < index.numCols; ++i )
    {
        switch ( index.attrType[i] )
        {
            case 'c':
            {
                int len = EncodeCString((char*)values[i], keyPtr, remaining_len);
                remaining_len -= len;
                keyPtr += len;
                break;
            }
            case INT:
            {
                // assert(spaceLeft >= 4);
                EncodeInt(*(int*)values[i], keyPtr);
                remaining_len -= 4;
                keyPtr += 4;
                break;
            }
            case LONG:
            {
                // assert(spaceLeft >= 8);
                EncodeLong(*(long*)values[i], keyPtr);
                remaining_len -= 8;
                keyPtr += 8;
                break;
            }
            case FLOAT:
            {
                // assert(spaceLeft >= 4);
                EncodeFloat(*(float*)values[i], keyPtr);
                remaining_len -= 4;
                keyPtr += 4;
                break;
            }
            default:
            {
                printf("Unknown type %c in query index\n", index.attrType[i]);
                exit(1);
            }
        }
    }
    int encoded_len = 256 - remaining_len;

    int scanDesc = AM_OpenIndexScan(index.fileDesc, index.attrType, index.attrLen, index.numCols, op, key);
    while(true){
        int rid = AM_FindNextEntry(scanDesc);
        if(rid == AME_EOF) break;
        char record[MAX_PAGE_SIZE];
        int num_bytes = Table_Get(table, rid, record, MAX_PAGE_SIZE);
        t->addRowFromByte(record, encoded_len, 1);
    }
    AM_CloseIndexScan(scanDesc);

    return t;
}

std::string Table::encodeTable() {
    std::string str;
    char* r = new char[db_name.size() + 2];
    int len = EncodeCString((char*)db_name.c_str(), r, db_name.size()+2);
    str.append(r,len);
    free(r);
    r = new char[name.size() + 2];
    len = EncodeCString((char*)name.c_str(), r, name.size()+2);
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
        r = (char*)malloc(sizeof(int));
        EncodeInt(indexes[i].numCols, r);
        str.append(r,4);
        free(r);
        for (int j = 0; j < indexes[i].numCols; ++j) {
            r = (char*) malloc(sizeof(char));
            EncodeInt(indexes[i].attrType[j], r);
            str.append(r,4);
            free(r);
            r = (char*) malloc(sizeof(int));
            EncodeInt(indexes[i].attrLen[j], r);
            str.append(r,4);
            free(r);
        }
    }
    return str;
}


Table decodeTable(byte* s, int max_len ) {
    char* r;
    int len = DecodeCString2(s, &r, max_len);
    std::string db_name(r,len);
    std::cout<<len<<std::endl;
    free(r);
    s += len+2;
    len = DecodeCString2(s, &r, max_len);
    std::cout<<len<<std::endl;
    std::string name(r,len);
    free(r);
    s += len+2;
    Schema schema = decodeSchema(s, max_len, &len);
    std::cout<<name<<' '<<db_name<<std::endl;
    schema.print();
    s+=len;
    int num_indexes = DecodeInt(s);
    std::cout<<num_indexes<<std::endl;
    s += sizeof(int);
    std::vector<IndexData> indexes(num_indexes);
    for(int i=0; i<num_indexes; i++) {
        indexes[i].indexNo = DecodeInt(s);
        s += sizeof(int);
        indexes[i].numCols = DecodeInt(s);
        s += sizeof(int);
        indexes[i].attrType = (char*) malloc(sizeof(char) * indexes[i].numCols);
        indexes[i].attrLen = (int*) malloc(sizeof(int) * indexes[i].numCols);
        for (int j = 0; j < indexes[i].numCols; ++j) {
            indexes[i].attrType[j] = DecodeInt(s);
            s += sizeof(int);
            indexes[i].attrLen[j] = DecodeInt(s);
            s += sizeof(int);
        }
    }
    return Table(&schema, (char*)name.c_str(), (char*)db_name.c_str(), false, indexes);
}
