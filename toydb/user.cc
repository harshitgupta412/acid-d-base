#include <stdio.h>
#include <string.h>
#include "user.h"
#include <iostream>

using namespace std;


User::User(string username, string password){
    uname = username;
    user_table_name = "DB_USER_TABLE";
    status = true;
    is_admin = false;

    user_table = connectUserTable();
    // user_table->print();

    char *user = new char[username.length()];
    strcpy(user,username.c_str());

    void** pk_s = new void*[1];
    pk_s[0] = (void*) user;
    void** data = user_table->getRow(pk_s);

    if (data == NULL) {
        status = false;
        user_table->close();
        return;
    }
    long pass = DecodeLong((char*)data[1]);
    hash<string> hasher;
    long hashedPasswordGuess = hasher(password);

    if (hashedPasswordGuess != pass){
        std::cout << "Incorrect username or password" << std::endl;
        status = false;
        user_table->close();
        return;
    }
    is_admin = (bool) DecodeInt((char*)data[2]);
    // user_table->print();
    user_table->close();
}

bool User::isAdmin() {
    return is_admin;
}
bool User::get_status() {
    return status;
}
bool User::addUser(std::string username, std::string password){
    if (!is_admin || username == "SUPERUSER") return false;

    char user[username.length()];
    strcpy(user,username.c_str());

    hash<string> hasher;
    std::string pass = std::to_string(hasher(password));
    char* passCh = new char[pass.length() + 1];
    strcpy(passCh, pass.c_str());
    
    void* data[3];
    char is_admin[] = "0";
    data[0] = (void*) user; data[1] = (void*) passCh; data[2] = (void*) is_admin;

    user_table = connectUserTable();
    bool ret = user_table->addRow(data, true);
    user_table->close();
    return ret;
}

std::string User::get_user() {
    return uname;
}

bool User::assignPerm(User& user, std::string dbName, int perm) {
    if (!is_admin) return false;

    user_table = connectUserPrivDb();
    if (perm < 0 || perm > 2) return false;

    void* data[3];
    data[0] = (void*) user.uname.c_str();
    data[1] = (void*) dbName.c_str();
    data[2] = (void*) std::to_string(perm).c_str();

    bool ret = user_table->addRow(data,true);
    // user_table->print();
    user_table->close();
    return ret;
}   

bool User::assignPerm(User& user, std::string dbName, std::string tableName, int perm) {
    if (!is_admin) return false;

    user_table = connectUserPrivTable();
    if (perm < 0 || perm > 2) return false;

    void* data[4];
    data[0] = (void*) user.uname.c_str();
    data[1] = (void*) dbName.c_str();
    data[2] = (void*) tableName.c_str();
    data[3] = (void*) std::to_string(perm).c_str();

    bool ret = user_table->addRow(data,true);
    // user_table->print();
    user_table->close();
    return ret;
}   

bool User::isAllowed(std::string dbName, int perm) {
    if (is_admin)
        return true;
    user_table = connectUserPrivDb();

    void** pk_s = new void*[2];
    pk_s[0] = (void*) uname.c_str();
    pk_s[1] = (void*) dbName.c_str();
    void** data = user_table->getRow(pk_s);

    if (data == NULL) {
        user_table->close();
        delete[] pk_s;
        return false;
    }

    int user_perm = DecodeInt((char*)data[2]);
    user_table->close();
    delete[] pk_s;
    return user_perm >= perm;
}

bool User::isAllowed(std::string dbName, std::string tableName, int perm) {
    if (is_admin)
        return true;
    if (isAllowed(dbName, perm))
        return true;

    user_table = connectUserPrivTable();

    void** pk_s = new void*[3];
    pk_s[0] = (void*) uname.c_str();
    pk_s[1] = (void*) dbName.c_str();
    pk_s[2] = (void*) tableName.c_str();
    void** data = user_table->getRow(pk_s);

    if (data == NULL) {
        user_table->close();
        delete[] pk_s;
        return false;
    }

    int user_perm = DecodeInt((char*)data[3]);
    user_table->close();
    delete[] pk_s;
    return user_perm >= perm;
}

Table* connectUserTable() {
    // Create table of users
    char user_table_name[] = "DB_USER_TABLE";
    char user_db_name[] = "DB_USER_DB";

    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"username", VARCHAR});
    cols.push_back({"password", LONG});
    cols.push_back({"admin", INT});
    vector<int> pk = {0};

    Schema* schema = new Schema(cols, pk);
    vector<IndexData> vi;
    Table* user_table = new Table(schema, user_table_name, user_db_name, false, vi,false);
    return user_table;
}

Table* connectUserPrivTable() {
    // Create user privileges for table
    char user_table_name[] = "DB_USER_PRIV_TABLE";
    char user_db_name[] = "DB_USER_DB";

    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"username", VARCHAR});
    cols.push_back({"DBNAME", VARCHAR});
    cols.push_back({"TABLE", VARCHAR});
    cols.push_back({"PRIV_LEVEL", INT});
    vector<int> pk = {0,1,2};
    // vector<int> pk = {0};

    Schema* schema = new Schema(cols, pk);
    vector<IndexData> vi;
    Table* user_table = new Table(schema, user_table_name, user_db_name, false, vi,false);
    return user_table;
}

Table* connectUserPrivDb() {
    // Create user privileges for db
    char user_table_name[] = "DB_USER_PRIV_DB";
    char user_db_name[] = "DB_USER_DB";

    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"username", VARCHAR});
    cols.push_back({"DBNAME", VARCHAR});
    cols.push_back({"PRIV_LEVEL", INT});
    vector<int> pk = {0,1};
    // vector<int> pk = {0};

    Schema* schema = new Schema(cols, pk);
    vector<IndexData> vi;
    Table* user_table = new Table(schema, user_table_name, user_db_name, false, vi,false);
    return user_table;
}

bool createUserDb() {
    Table* user_table = connectUserTable();

    char username[] = "SUPERUSER";
    hash<string> hasher;
    long passLong = hasher("SUPERUSER_PASSWORD");
    string pass = std::to_string(passLong);
    char* password = new char[pass.length() + 1];
    strcpy(password, pass.c_str());
    char admin[] = "1";

    void* data[3];
    data[0] = (void*) username;
    data[1] = (void*) password;
    data[2] = (void*) admin;
    bool ret =  user_table->addRow(data,true);
    user_table->close();
    return ret;
}

void createPrivilegeTable() {
    // Create user privileges for a given table
    Table* user_table = connectUserPrivTable();
    user_table->close();
}


void createPrivilegeDb() {
    Table* user_table = connectUserPrivDb();
    user_table->close();
}