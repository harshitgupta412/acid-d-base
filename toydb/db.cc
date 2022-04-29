#include <stdio.h>
#include <string.h>
#include "db.h"
#include <iostream>
using namespace std;

Database::Database(User* current_user){
    user = current_user;
    fail = false;
    current = "";
    db_table_name = "DB_TABLE";
    db_cross_name = "DB_CROSS_TABLE";
}

bool Database::connect(std::string name) {
    db_table = connectDbList();

    char *db_n = new char[name.length()+1];
    strcpy(db_n,name.c_str());

    void** pk = new void*[1];
    pk[0] = (void*) db_n;

    void** data = db_table->getRow(pk);

    if (data != NULL){
        current = (char*) data[0];
        db_table->close();
        return true;
    }
    else {
        fail = true;
        db_table->close();
        return false;
    }
}

bool Database::create(std::string name) {
    db_table = connectDbList();
    if (fail){
        return false;
    }
    char *db_n = new char[name.length()+1];
    strcpy(db_n,name.c_str());

    void** data = new void*[1];
    data[0] = (void*) db_n;

    bool status = db_table->addRow(data, true);

    if (status){
        current = name;
    }
    // db_table->print();
    db_table->close();
    return status;
}

bool Database::drop(){
    db_table = connectDbList();
    if (fail){
        return false;
    }
    char *db_n = new char[current.length()+1];
    strcpy(db_n,current.c_str());

    void** pk = new void*[1];
    pk[0] = (void*) db_n;

    bool status = db_table->deleteRow(pk);
    // db_table->print();
    db_table->close();
    current = "";
    return status;
}

Table* Database::load(std::string name){

    if (fail){
        return NULL;
    }

    db_cross_table = connectDbTableList();

    char *table_n = new char[name.length()+1];
    strcpy(table_n, name.c_str());

    char *db_n = new char[current.length()+1];
    strcpy(db_n, current.c_str());

    void** pk = new void*[2];
    pk[0] = (void*) db_n;
    pk[1] = (void*) table_n;

    char* data;
    int len;

    db_cross_table->getRecordAsBytes(pk, &data, &len);
    if (data == NULL){
        db_cross_table->close();
        return NULL;
    }

    int bytes_decoded = 0;
    char* cursor = data;

    short string_len = DecodeShort(cursor);
    char* db_record = (char *)malloc(string_len+1);
    
    int strlen = DecodeCString(cursor, db_record, len);
    cursor += strlen+2;
    bytes_decoded += strlen+2;
    len -= strlen+2;

    string_len = DecodeShort(cursor);
    char* table_record = (char *)malloc(string_len+1);
    strlen = DecodeCString(cursor, table_record, len);
    cursor += strlen+2;
    bytes_decoded += strlen+2;
    len -= strlen+2;

    char* metadata = new char[len];
    memcpy(metadata, cursor, len);


    Table r =  (decodeTable(metadata, MAX_PAGE_SIZE));
    Table* t = new Table(&r);
    return t;

}

bool Database::createTable(Table *t) {
    if (fail){
        return false;
    }

    if(t->get_db_name() != current)
    {
        std::cout << current << std::endl;
        std::cout << t->get_db_name() << std::endl;
        std::cout << "Error: Connected to a different database" << std::endl;
        return false;
    }

    db_cross_table = connectDbTableList();

    std::string name = t->get_table_name();

    std::string encoded = t->encodeTable();

    char *table_name = new char[name.size() + 1];
    strcpy(table_name, name.c_str());

    char *db_name = new char[current.size() + 1];
    strcpy(db_name, current.c_str());

    char *record = new char[MAX_PAGE_SIZE];
    char *initial = record;
    int spaceLeft = MAX_PAGE_SIZE;

    int bytes_encoded = 0;

    int len = EncodeCString(db_name, record, spaceLeft);

    spaceLeft -= len;
    record += len;
    bytes_encoded += len;

    int len2 = EncodeCString(table_name, record, spaceLeft);

    spaceLeft -= len2;
    record += len2;
    bytes_encoded += len2;

    memcpy(record, encoded.c_str(), encoded.size());
    int len3 = encoded.size();

    bool status = db_cross_table->addRowFromByte(initial, len + len2 + len3, true);
    // db_cross_table->print();
    db_cross_table->close();
    return status;

}

bool Database::deleteTable(Table *t) {
    if (fail){
        return false;
    }

    if(t->get_db_name() != current)
    {
        std::cout << "Error: Connected to a different database" << std::endl;
        return false;
    }

    db_cross_table = connectDbTableList();

    std::string name = t->get_table_name();

    char *table_name = new char[name.length()+1];
    strcpy(table_name,name.c_str());

    char *db_name = new char[current.length()+1];
    strcpy(db_name, current.c_str());

    void** data = new void*[2];
    data[0] = (void*) db_name;
    data[1] = (void*) table_name;

    bool status = db_cross_table->deleteRow(data);
    // db_cross_table->print(); 
    db_cross_table->close();
    return status; 
}

bool Database::isAllowed(std::string dbName, int perm) {
    return user->isAllowed(dbName, perm);
}

bool Database::isAllowed(std::string dbName, std::string tableName, int perm) {
    return user->isAllowed(dbName, tableName, perm);
}

bool isDb(std::string dbName) {
    Table* db_table = connectDbList();

    void** pk_s = new void*[1];
    pk_s[0] = (void*) dbName.c_str();
    void** data = db_table->getRow(pk_s);

    db_table->close();
    delete[] pk_s;

    return data != NULL;
}

bool isTable(std::string dbName, std::string tableName) {
    Table* db_cross_table = connectDbTableList();

    void** pk_s = new void*[2];
    pk_s[0] = (void*) dbName.c_str();
    pk_s[1] = (void*) tableName.c_str();
    void** data = db_cross_table->getRow(pk_s);

    db_cross_table->close();
    delete[] pk_s;

    return data != NULL;
}

Table* connectDbList() {
    // Create table of users
    char db_table_name[] = "DB_TABLE";
    char db_name[] = "DB";
    
    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"DBNAME", VARCHAR});
    std::vector<int> pk = {0};

    Schema *schema = new Schema(cols, pk);
    vector<IndexData> vi;
    Table* user_table = new Table(schema, db_table_name, db_name, false, vi, 0);
    return user_table;
}

Table* connectDbTableList() {
    // Create table of users
    char db_cross_name[] = "DB_CROSS_TABLE";
    char db_name[] = "DB";
    
    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"DBNAME", VARCHAR});
    cols.push_back({"TABLE", VARCHAR});  
    cols.push_back({"METADATA", VARCHAR}); 
    std::vector<int> pk = {0, 1};

    Schema *schema = new Schema(cols, pk);
    vector<IndexData> vi;
    Table* user_table = new Table(schema, db_cross_name, db_name, false, vi, 0);
    return user_table;
}

bool createDbList() {
    Table* dbList = connectDbList();
    char db[] = "DB";
    void* data[1];
    data[0] = (void*) db;
    bool ret = dbList->addRow(data, true, true);
    if(!ret) {
        dbList->rollback();
        return false;
    }

    char db1[] = "DB_USER_DB";
    data[0] = (void*) db1;
    ret = dbList->addRow(data, true, true);
    if(!ret) {
        dbList->rollback();
        return false;
    }
    dbList->close();
    return true;
}

bool addTableToList(std::string dbName, std::string tableName, Table* t) {
    Table* dbTableList = connectDbTableList();

    char *record = new char[MAX_PAGE_SIZE];
    char *initial = record;
    int spaceLeft = MAX_PAGE_SIZE;
    int bytes_encoded = 0;
    int len = EncodeCString((char*)dbName.c_str(), record, spaceLeft);
    spaceLeft -= len;
    record += len;
    bytes_encoded += len;
    int len2 = EncodeCString((char*)tableName.c_str(), record, spaceLeft);
    spaceLeft -= len2;
    record += len2;
    bytes_encoded += len2;
    std::string encoded = t->encodeTable();
    memcpy(record, encoded.c_str(), encoded.size());
    int len3 = encoded.size();
    bool ret = dbTableList->addRowFromByte(initial, len + len2 + len3, true);
    dbTableList->close();
    return ret;
}

bool createDbTableList() {
    addTableToList("DB", "DB_TABLE", connectDbList());
    addTableToList("DB", "DB_CROSS_TABLE", connectDbTableList());
    addTableToList("DB_USER_DB", "DB_USER_TABLE", connectUserTable());
    addTableToList("DB_USER_DB", "DB_USER_PRIV_TABLE", connectUserPrivTable());
    addTableToList("DB_USER_DB", "DB_USER_PRIV_DB", connectUserPrivDb());
}
