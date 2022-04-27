#include <stdio.h>
#include <string.h>
#include "db.h"
#include <iostream>

using namespace std;

Database::Database(){
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
    std::vector<int> pk2 = {0, 1};

    schema_cross = new Schema(cols2, pk2);
    vector<IndexData> vi;

    db_table = new Table(schema_db, (char*)db_table_name.c_str(), db_name, false, vi);
    db_cross_table = new Table(schema_cross, (char*)db_cross_name.c_str(), db_name, false, {});
}

bool Database::connect(std::string name, User *current_user) {
    char user[name.length()];
    strcpy(user,name.c_str());
    void** data = db_table->getRow((void*) user);

    if (data != NULL){
        current = (char*) data[0];
        return true;
    }
    else {
        fail = true;
        return false;
    }
}

// to-do

// bool Database::createTable(Table *t) {
//     char table_name[name.length()];
//     strcpy(table_name,name.c_str());
//     void** data = (*db_table_name).getRow((void*) table);

//     if (data != NULL){
//         current = (char*) data[0];
//         return true;
//     }
//     else {
//         fail = true;
//         return false;
//     }
// }
// bool Database::create_table(Table* t) {
//     string name = t->name;
// }