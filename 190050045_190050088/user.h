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

    bool isAdmin();
    bool addUser(std::string username, std::string password);
    std::string get_user();
    // bool User::assignPerm(User& user, Database& db, int perm);
    // bool User::assignPerm(User& user, Table& tbl, int perm);

    
};


#endif
