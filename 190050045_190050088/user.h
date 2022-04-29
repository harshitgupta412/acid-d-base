#ifndef _USER_H_
#define _USER_H_

#include "table.h"
#include <string>
#include <iostream>
#include <vector>
#include <assert.h>

class User {
    Table *user_table;
    std::string user_table_name;
    Schema *schema;
    bool is_admin;
    bool status;
    std::string uname;

    public:

    User(std::string username, std::string password);

    bool get_status();
    bool isAdmin();
    bool addUser(std::string username, std::string password);
    std::string get_user();
    bool assignPerm(User& user, std::string dbName, int perm);
    bool assignPerm(User& user, std::string dbName, std::string tableName, int perm);

    // true if user has the following permission
    bool isAllowed(std::string dbName, int perm);
    bool isAllowed(std::string dbName, std::string tableName, int perm);
};

Table* connectUserTable();
Table* connectUserPrivTable();
Table* connectUserPrivDb();

bool createUserDb();
void createPrivilegeTable();
void createPrivilegeDb();

#endif
