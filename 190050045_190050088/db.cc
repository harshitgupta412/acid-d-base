#include <stdio.h>
#include <string.h>
#include "db.h"
#include <iostream>
using namespace std;

Database::Database(User* current_user){
    user = current_user;
    fail = 0;
    db_table_name = "DB_TABLE";
    db_cross_name = "DB_CROSS_TABLE";
    char* db_name = "DB";
    
    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"DBNAME", VARCHAR});
    std::vector<int> pk = {0};

    schema_db = new Schema(cols, pk);

    std::vector<std::pair<std::string, int> > cols2;
    cols2.push_back({"DBNAME", VARCHAR});
    cols2.push_back({"TABLE", VARCHAR});  
    cols2.push_back({"METADATA", VARCHAR});     
    std::vector<int> pk2 = {0, 1};

    schema_cross = new Schema(cols2, pk2);
    vector<IndexData> vi;

    db_table = new Table(schema_db, (char*)db_table_name.c_str(), db_name, false, vi);
    db_cross_table = new Table(schema_cross, (char*)db_cross_name.c_str(), db_name, false, {});
}

bool Database::connect(std::string name) {
    // todo check if user has permission
    char *db_n = new char[name.length()];
    strcpy(db_n,name.c_str());

    void** pk = new void*[1];
    pk[0] = (void*) db_n;

    void** data = db_table->getRow(pk);

    if (data != NULL){
        current = (char*) data[0];
        return true;
    }
    else {
        fail = true;
        return false;
    }
}

bool Database::create(std::string name) {
    if (fail){
        return false;
    }
    char *db_n = new char[name.length()];
    strcpy(db_n,name.c_str());

    void** data = new void*[1];
    data[0] = (void*) db_n;

    bool status = db_table->addRow(data, true);

    if (status){
        current = name;
    }

    return status;
}
bool Database::drop(){
    if (fail){
        return false;
    }
    char *db_n = new char[current.length()];
    strcpy(db_n,current.c_str());

    void** pk = new void*[1];
    pk[0] = (void*) db_n;

    return db_table->deleteRow(pk);
}

Table* Database::load(std::string name){
    if (fail){
        return NULL;
    }
    char *table_n = new char[name.length()];
    strcpy(table_n, name.c_str());

    char *db_n = new char[current.length()];
    strcpy(db_n, current.c_str());

    void** pk = new void*[2];
    pk[0] = (void*) table_n;
    pk[1] = (void*) db_n;

    void** data = db_cross_table->getRow(pk);

    if (data == NULL){
        return NULL;
    }

    Table ret = (decodeTable((char*) data[2], MAX_PAGE_SIZE));

    Table* t = new Table(ret);

    return t;


}

bool Database::createTable(Table *t) {
    if (fail){
        return false;
    }
    std::string name = t->get_name();
    string encoded = t->encodeTable();

    char *table_name = new char[name.length()];
    strcpy(table_name,name.c_str());

    char *db_name = new char[current.length()];
    strcpy(db_name, current.c_str());

    char *encoded_c = new char[encoded.length()];
    strcpy(db_name, encoded.c_str());

    void** data = new void*[2];
    data[0] = (void*) table_name;
    data[1] = (void*) db_name;
    data[2] = (void*) encoded_c;

    return db_cross_table->addRow(data, true);

}

bool Database::deleteTable(Table *t) {
    if (fail){
        return false;
    }
    std::string name = t->get_name();

    char *table_name = new char[name.length()];
    strcpy(table_name,name.c_str());

    char *db_name = new char[current.length()];
    strcpy(db_name, current.c_str());

    void** data = new void*[2];
    data[0] = (void*) table_name;
    data[1] = (void*) db_name;

    return db_cross_table->deleteRow(data);
    
}
