
#include <stdio.h>
#include <string.h>
#include <sstream>
#include "table.h"
#include <dirent.h>
#include <sys/types.h>
#define checkerr(err, message) {if (err < 0) {std::cerr << (message) << std::endl; exit(1);}}

#define SLOT_OFFSET 2
#define HEADER_SIZE_OFFSET 2
#define SIZE_OFFSET 2

#define END_OF_PAGE PF_PAGE_SIZE

int count = 0;


static int count_files(const char *path) {
    struct dirent *entry;
    DIR *dir = opendir(path);
    if (dir == NULL) {
        return 0;
    }

    int count = 0;
    while ((entry = readdir(dir)) != NULL) {
       count++;
    }

    closedir(dir);
    return count;
}
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

std::vector<int> indexNo_to_cols(int indexNo, int maxcols, int numCols) 
{
    std::vector<int> cols;
    for(int i = 0; i < numCols; i++)
    {
        cols.push_back(indexNo % maxcols);
        indexNo /= maxcols;
    }
    return cols;
}

char* cols_to_char(int attrLength[], std::vector<int> cols, Schema_ *sch, char* record) {
    char* result = new char[MAX_PAGE_SIZE];
    memset(result, 0, MAX_PAGE_SIZE*sizeof(char));
    int encoded_len = 0;
    for(int i = 0; i < cols.size(); i++) {
        char* res = getNthfield(record, cols[i], sch);
        memcpy(result, res, attrLength[i]);
        encoded_len += attrLength[i];
        result += attrLength[i];
    }
    return result-encoded_len;
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

void Table::deleteRow(int rowId, bool log){
    char record[MAX_PAGE_SIZE];
    int num_bytes = Table_Get(table, rowId, record, MAX_PAGE_SIZE);
    if(num_bytes == -1) {
        return;
    }
    
    if(log){
        std::string val(record, num_bytes);
        table_logs.push_back({'d',val});
    }

    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();   
            // std::cout<<indexes[i].indexNo<<std::endl;
            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, record);

            int err = AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rowId);
            // std::cout<<"delte "<<err<<std::endl;
            assert(err == AME_OK);
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(db_name + "." + name + "." + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            assert(indexes[i].fileDesc >= 0);
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();

            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, record);

            int err = AM_DeleteEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rowId);
            // std::cout<<"delte "<<err<<std::endl;
            if (err != AME_OK)
            {
                std::cout << "Error " << err << " deleteRow into index " << indexes[i].indexNo << std::endl;
                exit(1);
            }
            // assert(err == AME_OK);
        }
    }

    Table_Delete(table, rowId);
}

Table::Table(Schema* _schema, char* table_name, char* db_name, bool overwrite, std::vector<IndexData> _indexes, bool index_pk): schema(*_schema) {
    name += db_name;
    name += ".";
    name += table_name;
    name += ".tbl";
    this->db_name = db_name;
    if(Table_Open((char*)name.c_str(), (Schema_*)_schema->getSchema(), overwrite, &table)== -1){
        std::cout << "Error opening table " << name << std::endl;
        exit(1);
    }
    name = table_name;
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
        if (addDefaultIndex) createIndex(_pk);
    }

    if(overwrite) {
        for(auto index: indexes) {
            eraseIndex(index.indexNo);
            // std::cout<<index.indexNo<<std::endl;
            createIndex(indexNo_to_cols(index.indexNo, schema.getSchema()->numColumns, index.numCols));
        } 
    }
}

const Schema& Table::getSchema() {
    return schema;
}

Table_* Table::getTable() {
    return table;
}

bool Table::addRow(void* data[], bool update, bool log) {
    byte** pk_value = new byte*[pk_size];
    for(int i=0; i<pk_size; i++) {
        pk_value[i] = (byte*)data[pk_index[i]];
    }
    int rid = Table_Search(table, pk_index, pk_value, pk_size);
    if(rid != -1 && !update) return false;
    else if(rid != -1){
        // std::cout << rid << std::endl;
        deleteRow(rid, log);
    }
    char record[MAX_PAGE_SIZE];
    Schema_ sch = *schema.getSchema();

    int len = encode(&sch, (char**)data, record, MAX_PAGE_SIZE);
    // add to log before continuing
    Table_Insert(table, record, len, &rid);

    if(log){
        table_logs.push_back({'a', std::to_string(rid)});
    }

    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();
            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, record);
/*
            for(int j =0; j< indexes[i].numCols; j++)
                std::cout << indexes[i].attrLen[j] << " ";
            std::cout << std::endl;
*/
            int err = AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rid);
            if (err != AME_OK)
            {
                std::cout << "Error " << err << " inserting into index " << indexes[i].indexNo << std::endl;
                exit(1);
            }
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(db_name + "." + name + "." + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            // std::cout<<(name + std::to_string(indexes[i].indexNo) + ".idx")<<std::endl;
            assert(indexes[i].fileDesc >= 0);
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();

            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, record);

            int err = AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rid);
            if (err != AME_OK)
            {
                std::cout << "Error " << err << " inserting into index " << indexes[i].indexNo << std::endl;
                exit(1);
            }
        }
    }
    return true;
}

bool Table::addRowFromByte(byte *data, int len, bool update) {
    byte** pk_value = new byte*[pk_size];
    for(int i=0; i<pk_size; i++) {
        char* src = (byte*)getNthfield(data, pk_index[i], (Schema_ *)schema.getSchema());
        switch ( schema.getSchema()->columns[pk_index[i]].type )
        {
            case VARCHAR:
            {
                short string_len = DecodeShort(src);
                pk_value[i] = (char *)malloc(string_len+1);
                int strlen = DecodeCString(src, pk_value[i], len);
                break;
            }
            case INT:
            {
                pk_value[i] = (char*)malloc(sizeof(int));
                memcpy(pk_value[i],src, 4);
                break;
            }
            case LONG:
            {
                pk_value[i] = (char*)malloc(sizeof(long));
                memcpy(pk_value[i],src, 8);
                break;
            }
            case FLOAT:
            {
                pk_value[i] = (char*)malloc(sizeof(float));
                memcpy(pk_value[i],src, 4);
                break;
            }
            default:
            {
                printf("Unknown type %d\n", schema.getSchema()->columns[pk_index[i]].type);
                exit(1);
            }
        }
    }
    // std::cout << "Table Search Starting" << std::endl;
    int rid = Table_Search(table, pk_index, pk_value, pk_size);
    // std::cout << ".Table Searched" << std::endl;
    if(rid != -1 && !update) return false;
    else if(rid != -1){
        // std::cout << rid << std::endl;
        deleteRow(rid);
    }
    Schema_ sch = *schema.getSchema();

    
    Table_Insert(table, data, len, &rid);
    for(int i=0; i<indexes.size(); i++){
        if(indexes[i].isOpen){
            Schema_ sch = *schema.getSchema();
            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, data);

            int err = AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rid);
            if (err != AME_OK)
            {
                std::cout << "Error " << err << " deleting from index " << indexes[i].indexNo << std::endl;
                exit(1);
            }
        }
        else {
            indexes[i].fileDesc = PF_OpenFile((char*)(db_name + "." + name + "." + std::to_string(indexes[i].indexNo) + ".idx").c_str());
            assert(indexes[i].fileDesc >= 0);
            indexes[i].isOpen = true;
            Schema_ sch = *schema.getSchema();

            std::vector<int> cols = indexNo_to_cols(indexes[i].indexNo, sch.numColumns, indexes[i].numCols);
            char* result = cols_to_char(indexes[i].attrLen, cols, &sch, data);

            int err = AM_InsertEntry(indexes[i].fileDesc, indexes[i].attrType, indexes[i].attrLen, indexes[i].numCols, result, rid);
            if (err != AME_OK)
            {
                std::cout << "Error " << err << " deleting from index " << indexes[i].indexNo << std::endl;
                exit(1);
            }
        }
    }
    return true;
}

std::string Table::get_name(){
    return db_name + "." + this->name;
}
std::string Table::get_db_name(){
    return db_name;
}
std::string Table::get_table_name(){
    return name;
}

bool Table::deleteRow(void** pk, bool log) {
    int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
    if(rid == -1) return true;
    deleteRow(rid, log);
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
void Table::getRecordAsBytes(void** pk, char**record, int* len) {
    int rid = Table_Search(table, pk_index, (byte**)pk, pk_size);
    if(rid == -1) return;
    *record = new char[MAX_PAGE_SIZE];
    *len = Table_Get(table, rid, *record, MAX_PAGE_SIZE);
}

void Table::print() {
    Schema_ sch = *schema.getSchema(); 
    schema.print();
    Table_Scan(table, &sch, print_row);
}

std::string get_temp_name(){
    std::stringstream ss;
    ss << "result";
    ss << count_files("temp/"); 
    return ss.str();
}
Table* Table::query(bool (*callback)(RecId, byte*, int)){

    Table *t = new Table(&schema, (char*)(get_temp_name().c_str()), (char*)("temp/" + this->db_name).c_str(), true, {}, false);
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
                    t->addRowFromByte(pageBuf + getNthSlotOffset(i, pageBuf)+2, len, true);
                }
            }
            if (err != PFE_PAGEFIXED) checkerr(PF_UnfixPage(table->fileDesc, pageNo, 0), "Table_ scan : unfix page");
            err = PF_GetNextPage(table->fileDesc, &pageNo, &pageBuf);
        } while( err >= 0 || err == PFE_PAGEFIXED );
        assert(err == PFE_EOF);
    }
        else
            checkerr(err, "Table_ scan : get first page");

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
    index.isOpen = false;
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
    int err = AM_CreateIndex((char*)(db_name + "." + name).c_str(), index.indexNo, index.attrType, index.attrLen, index.numCols);
    if(err == AME_PF) {
        int fd = PF_OpenFile((char*)(db_name + "." + name + "." + std::to_string(index.indexNo) + ".idx").c_str());
        if(fd >= 0) {
            PF_CloseFile(fd);
        }
        else {
            fprintf(stderr, "Table createIndex : cannot create index. Your PF layer sucks\n");
            exit(1);
        }
    }
    indexes.push_back(index);
    return indexNo;
}

bool Table::eraseIndex(int id) {
    for(int i=0; i<indexes.size(); i++) {
        if(indexes[i].indexNo == id) {
            if(indexes[i].isOpen) {
                int err = PF_CloseFile(indexes[i].fileDesc);
                // std::cout<<"table close "<<err<<std::endl;
                assert(err == PFE_OK);
                indexes[i].isOpen = false;
            }
            int err = AM_DestroyIndex((char*)(db_name + "." + name).c_str(), id);
            assert(err == AME_OK);
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
            // assert(PF_UnfixPage(indexes[i].fileDesc, pageNo, 0) == PFE_OK);
            int err = PF_CloseFile(indexes[i].fileDesc);
            // std::cout<<"table close "<<err<<std::endl;
            assert(err == PFE_OK);
            indexes[i].isOpen = false;
        }
    }
    table = NULL;
    return true;
}

Table::~Table() {
    // if(table != NULL)
    //     close();
    free (pk_index);
    free (table);
}

Table* Table::queryIndex(int indexNo, int op, std::vector<void*> values)
{
    Table *t = new Table(&schema, (char*)(get_temp_name().c_str()), (char*)("temp/" + this->db_name).c_str(), false, std::vector<IndexData>(), false);
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

    // byte key[256];
    byte* keyPtr = new byte[256];
    memset(keyPtr, 0, 256*sizeof(char));
    int remaining_len = 256;
    for ( int i = 0; i < index.numCols; ++i )
    {
        switch ( index.attrType[i] )
        {
            case 'c':
            {
                int len = EncodeCString((char*)values[i], keyPtr, remaining_len);
                remaining_len -= MAX_VARCHAR_LENGTH;
                keyPtr += MAX_VARCHAR_LENGTH;
                break;
            }
            case 'i':
            {
                // assert(spaceLeft >= 4);
                EncodeInt(*(int*)values[i], keyPtr);
                remaining_len -= 4;
                keyPtr += 4;
                break;
            }
            case 'l':
            {
                // assert(spaceLeft >= 8);
                EncodeLong(*(long*)values[i], keyPtr);
                remaining_len -= 8;
                keyPtr += 8;
                break;
            }
            case 'f':
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
    if(!index.isOpen) {
        index.fileDesc = PF_OpenFile((char*)(db_name + "." + name + "." + std::to_string(index.indexNo) + ".idx").c_str());
        assert(index.fileDesc >= 0);
        index.isOpen = true;    
    }
    int scanDesc = AM_OpenIndexScan(index.fileDesc, index.attrType, index.attrLen, index.numCols, op, keyPtr - encoded_len);
    if(scanDesc < 0) {
        fprintf(stderr, "AM_OpenIndexScan failed in query index\n");
        exit(1);
    }
    int x = 0;
    while(true){
        int rid = AM_FindNextEntry(scanDesc);
        if(rid == AME_EOF) break;
        char record[MAX_PAGE_SIZE];
        std::cout<<rid<<std::endl;
        int num_bytes = Table_Get(table, rid, record, MAX_PAGE_SIZE);
        // std::cout << "Row fetched " << num_bytes << std::endl;
        t->addRowFromByte(record, num_bytes, 1);
        // std::cout << "Row added" << std::endl;
        x++;
    }
    std::cout<<x<<std::endl;
    AM_CloseIndexScan(scanDesc);
    // delete keyPtr;
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

void Table::rollback() 
{
    int i = table_logs.size();
    return;
    i--;
    for(i; i>=0; i--) {
        std::pair<char, std::string> log = table_logs[i];
        if(log.first == 'a') {
            deleteRow(stoi(log.second), false);
            // std::cout << "Rollback Delete: rid " << stoi(log.second) << std::endl;
            // std::cout << "Rollback: Deleting row" << std::endl;
        }
        else {
            // std::cout << "Rollback Add: " << std::endl;
            addRowFromByte((char*)log.second.c_str(), log.second.length(), true);
            // std::cout << "Rollback: Adding row" << std::endl;
        }
    }
    table_logs.clear();
}

void Table::clear_logs() {
    table_logs.clear();
}


Table decodeTable(byte* s, int max_len ) {
    char* r;
    int len = DecodeCString2(s, &r, max_len);
    std::string db_name(r,len);
    
    free(r);
    s += len+2;
    len = DecodeCString2(s, &r, max_len);
   
    std::string name(r,len);
    free(r);
    s += len+2;
    Schema schema = decodeSchema(s, max_len, &len);

    s+=len;
    int num_indexes = DecodeInt(s);
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

void append(void* callbackObj, int rid, byte* row, int len){
    std::vector<std::pair<int, std::string> > * p = (std::vector<std::pair<int, std::string> > *) callbackObj;
    std::string str(row, len+2);
    p->push_back({rid, str});
}
std::vector<std::pair<int, void**>> Table::get_records(){
    std::vector<std::pair<int, std::string> > returnal;
    std::vector<std::pair<int, void**>> final_res;
    Table_Scan(table, &returnal, append);

    Schema_ *sch = schema.getSchema();
    for (std::pair<int, std::string> p : returnal){
        char** data = new char*[sch->numColumns];
        decode(sch, data, (char*)p.second.c_str()+2, p.second.length());
        // for(int i=0; i<sch->numColumns; i++) {
        //     std::cout << data[i] << " ";
        // }
        // std::cout << std::endl;
        final_res.push_back({p.first, (void**)data});

    }
    
    return final_res;
}

std::vector<std::pair<int, void**>> Table::get_records2(){
    std::vector<std::pair<int, std::string> > returnal;
    std::vector<std::pair<int, void**>> final_res;
    Table_Scan(table, &returnal, append);

    Schema_ *sch = schema.getSchema();
    for (std::pair<int, std::string> p : returnal){
        char** data = new char*[sch->numColumns];
        void** res = new void*[sch->numColumns];
        decode(sch, data, (char*)p.second.c_str()+2, p.second.length());
        for(int i =0; i< sch->numColumns; i++) {
            switch(sch->columns[i].type){
                case INT: {
                    int in = DecodeInt(data[i]);
                    void* s = (void*) std::to_string(in).c_str();
                    res[i] = new void*;
                    memcpy(res[i], s, 4);
                    break;
                }
                case FLOAT: {
                    float f = DecodeFloat(data[i]);
                    void* s = (void*) std::to_string(f).c_str();
                    res[i] = new void*;
                    memcpy(res[i], s, 4);
                    break;
                }
                case LONG: {
                    long l = DecodeLong(data[i]);
                    void* s = (void*) std::to_string(l).c_str();
                    res[i] = new void*;
                    memcpy(res[i], s, 8);
                    break;
                }
                case VARCHAR: {
                    res[i] = (void*) data[i];
                    break;
                }
            }
        }
        final_res.push_back({p.first, res});

    }
    
    return final_res;
}

Table* Table::project(std::vector<int> cols) {
    for(auto col: cols){
        if (col < 0 || col >= schema.getSchema()->numColumns){
            std::cout << "Invalid columns to project on" << std::endl;
            return NULL;
        }
    }

    std::vector<std::pair<std::string, int> > schema_cols;
    Schema_ sch = *schema.getSchema();
    for(auto col: cols)
        schema_cols.push_back({sch.columns[col].name, sch.columns[col].type});

    Schema *projectSchema = new Schema(schema_cols, cols);
    Table* projectTable = new Table(projectSchema, (char*)(get_temp_name().c_str()), (char*)("temp/" + db_name).c_str(), true, {}, false);

    std::vector<std::pair<int, void**> > rows = get_records();

    for(auto row: rows) {
        void** projectRow = new void*[cols.size()];
        for(int i = 0; i < cols.size(); i++) 
            projectRow[i] = row.second[cols[i]];
        projectTable->addRow(projectRow, true);
        delete[] row.second;
    }
    return projectTable;
}

void copyRow(void* callbackObj, int rid, byte* row, int len) {
    Table* t = (Table*) callbackObj;
    t->addRowFromByte(row+2, len, true);
}

Table* table_union(Table* t1, Table* t2) {
    Schema s1 = t1->getSchema();
    Schema s2 = t2->getSchema();

    if (t1->get_db_name() != t2->get_db_name()){
        std::cerr << "Tables belong to different databases" << std::endl;
        return NULL;
    }
    if (s1.getSchema()->numColumns != s2.getSchema()->numColumns){
        std::cerr << "Schema of the 2 tables do not match" << std::endl;
        return NULL;
    }
    for(int i = 0; i<s1.getSchema()->numColumns; i++) {
        if(s1.getSchema()->columns[i].type != s2.getSchema()->columns[i].type) {
            std::cerr << "Schema of the 2 tables do not match" << std::endl;
            return NULL;
        }
    }

    std::vector<int> pk;
    for(int i = 0; i<s1.getSchema()->numColumns; i++)
        pk.push_back(i);
    Schema* s = new Schema(s1.getSchema(), pk);
    Table* t = new Table(s, (char*)(get_temp_name().c_str()), (char*)("temp/" + t1->get_db_name()).c_str(), true, {}, false);
    Table_Scan(t1->getTable(), t, copyRow);
    Table_Scan(t2->getTable(), t, copyRow);

    // std::vector<std::pair<int, void**> > rows = t1->get_records2();
    // for(auto row: rows){
    //     t->addRow(row.second, true, false);
    // }

    // rows = t2->get_records2();
    // for(auto row: rows)
    //     t->addRow(row.second, true, false);
    return t;
}

Table* table_intersect(Table* t1, Table *t2) {
    Schema s1 = t1->getSchema();
    Schema s2 = t2->getSchema();

    if (t1->get_db_name() != t2->get_db_name()){
        std::cerr << "Tables belong to different databases" << std::endl;
        return NULL;
    }
    if (s1.getSchema()->numColumns != s2.getSchema()->numColumns){
        std::cerr << "Schema of the 2 tables do not match" << std::endl;
        return NULL;
    }
    for(int i = 0; i<s1.getSchema()->numColumns; i++) {
        if(s1.getSchema()->columns[i].type != s2.getSchema()->columns[i].type) {
            std::cerr << "Schema of the 2 tables do not match" << std::endl;
            return NULL;
        }
    }

    Table* t = new Table(&s1, (char*)(get_temp_name().c_str()), (char*)("temp/" + t1->get_db_name()).c_str(), true, {}, false);
    std::vector<std::pair<int, void**> > rows = t1->get_records();
    std::vector<int> pk_keys = s1.getpk();
    for(auto row: rows) {
        void* pk[pk_keys.size()];
        for(auto i: pk_keys)
            pk[i] = row.second[pk_keys[i]];
        void** res = t2->getRow(pk);
        if(res == NULL)
            continue;
        t->addRow(row.second, true);
    }

    return t;
}

Table* table_join(Table* t1, Table* t2, std::vector<int> cols1, std::vector<int> cols2){
    Schema s1 = t1->getSchema();
    Schema s2 = t1->getSchema();

    const Schema_ *sch1 = s1.getSchema();
    const Schema_ *sch2 = s2.getSchema();

    if (t1->get_db_name() != t2->get_db_name()){
        return NULL;
    }
    if (t1->get_db_name() != t2->get_db_name() || cols1.size() != cols2.size()){
        return NULL;
    }
    
    for (int i=0; i<cols1.size(); ++i){
        if (cols1[i] >= sch1->numColumns || cols2[i] >= sch2->numColumns || cols1[i] < 0 || cols2[i] < 0){
            return NULL;
        }
        if (sch1->columns[cols1[i]].type != sch2->columns[cols2[i]].type){
            return NULL;
        }
    }
    
    std::vector<std::pair<std::string, int> > cols_join;

    for (int i=0; i<sch1->numColumns; ++i){
        cols_join.push_back({t1->get_name() + "." + sch1->columns[i].name, sch1->columns[i].type});
    }
    for (int i=0; i<sch2->numColumns; ++i){
        cols_join.push_back({t2->get_name() + "." + sch2->columns[i].name, sch2->columns[i].type});
    }

    std::vector<int> pk;
    std::vector<int> pk1 = s1.getpk(); 
    std::vector<int> pk2 = s2.getpk();

    for (int i: pk1){
        pk.push_back(i);
    }
    for (int i: pk2){
        pk.push_back(sch1->numColumns + i);
    }
    Schema *common = new Schema(cols_join, pk);
    Table* t = new Table(common, (char*)(get_temp_name().c_str()), (char*)("temp/" + t1->get_db_name()).c_str(), true);

    std::vector<std::pair<int, void**>> t1_rows = t1->get_records();
    std::vector<std::pair<int, void**>> t2_rows = t2->get_records();


    for (std::pair<int, void**> r1: t1_rows){
        for (std::pair<int, void**> r2: t2_rows){
            std::vector<void*> d1(cols1.size());
            std::vector<void*> d2(cols2.size());

            void* union_row[sch1->numColumns + sch2->numColumns];

            int j = 0;
            for (int i=0; i<sch1->numColumns; ++i){
                union_row[i] = r1.second[i];
                if (i == cols1[j]){
                    d1[j] = r1.second[i];
                    j++;
                }
            }
            j = 0;
            for (int i=0; i<sch2->numColumns; ++i){
                union_row[sch1->numColumns + i] = r2.second[i];
                if (i == cols2[j]){
                    d2[j] = r2.second[i];
                    j++;
                }
            }
            bool match = true;
            int i1, i2;
            float f1, f2;
            long l1, l2;
            char *s1, *s2;
            for (int i=0; i<cols1.size(); ++i){
                switch(sch1->columns[cols1[i]].type){
                    case INT:
                        i1 =  DecodeInt((char*)d1[i]);
                        i2 =  DecodeInt((char*)d2[i]);
                        if (i1 != i2) match = false;
                        break;
                    case FLOAT:
                        f1 =  DecodeFloat((char*)d1[i]);
                        f2 =  DecodeFloat((char*)d2[i]);
                        if (f1 != f2) match = false;
                        break;
                    case LONG:
                        l1 =  DecodeLong((char*)d1[i]);
                        l2 =  DecodeLong((char*)d2[i]);
                        if (l1 != l2) match = false;
                        break;
                    case VARCHAR:
                        s1 = (char*)d1[i];
                        s2 = (char*)d2[i];
                        if (strcmp(s1, s2) != 0) match = false;
                        break;
                }
            }

            if (match){
                t->addRow(union_row, true);
            }
        }
    }
    return t;
}

Table::Table(Table* t2) : schema(&(t2->schema)) {
    name = t2->name;
    db_name = t2->db_name;
    pk_index = new int[t2->pk_size];
    memcpy(pk_index, t2->pk_index, pk_size*sizeof(int));
    pk_size = t2->pk_size;
    indexes = t2->indexes;
    table = new Table_;
    memcpy(table, t2->table, sizeof(Table_));
}