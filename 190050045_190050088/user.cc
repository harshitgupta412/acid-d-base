#include <stdio.h>
#include <string.h>
#include "user.h"
#include <iostream>

using namespace std;


User::User(string username, string password){
    uname = username;
    user_table_name = "DB_USER_TABLE";
    char* user_db_name = "DB_USER_DB";
    status = true;
    is_admin = false;

    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"username", VARCHAR});
    cols.push_back({"password", LONG});
    cols.push_back({"admin", INT});
    vector<int> pk = {0};

    schema = new Schema(cols, pk);
    vector<IndexData> vi;

    user_table = new Table(schema, (char*) user_table_name.c_str(), user_db_name, false, vi);

    char user[username.length()];
    strcpy(user,username.c_str());
    void** data = user_table->getRow((void*) user);

    if (data == NULL) {
        status = false;
        return;
    }
    int pass = DecodeLong((char*)data[1]);
    hash<string> hasher;

    long hashedPasswordGuess = hasher(password);

    if (hashedPasswordGuess != pass){
        status = false;
        return;
    }
    is_admin = (bool) DecodeInt((char*)data[2]);

}

bool User::isAdmin() {
    return is_admin;
}
bool User::addUser(std::string username, std::string password){
    if (!is_admin) return false;

    char user[username.length()];
    strcpy(user,username.c_str());

    char pass[password.length()];
    strcpy(pass,password.c_str());
    void* data[3];
    bool* is_admin = new bool;
    *is_admin = false;
    data[0] = (void*) user; data[1] = (void*) pass; data[2] = (void*) is_admin;

    return user_table->addRow(data, true);

}

std::string User::get_user() {
    return uname;
}
