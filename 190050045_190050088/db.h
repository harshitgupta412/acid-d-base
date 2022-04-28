#ifndef _DB_H_
#define _DB_H_

#include "user.h"
#include <string>
#include <iostream>
#include <vector>
#include <assert.h>

class Database {
    Table *db_table, *db_cross_table;
    std::string db_table_name, db_cross_name;
    Schema *schema_db, *schema_cross;
    int status; //commit status
    std::string current;
    int fail;
    User* user;

    public:
    Database(User *current_user);
    bool connect(std::string name);
    bool drop();
    bool create(std::string name);
    Table* load(std::string db_name);

    bool createTable(Table* t);
    bool deleteTable(Table* t);
    bool initTransaction();
    bool commit();
    bool rollback();
    
};

Table* connectDbList();
Table* connectDbTableList();

#endif
