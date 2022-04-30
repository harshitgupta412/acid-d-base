#include <bits/stdc++.h>
#include <utility>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "../schema.h"
#include "../table.h"
#include "../db.h"
#include "../user.h"

#define checkerr(err, message) {if (err < 0) {PF_PrintError(message); exit(1);}}

#define MAX_PAGE_SIZE 4000
#define MAX_TOKENS 100
#define MAX_LINE_LEN   1000

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0.idx"
#define CSV_NAME "./dblayer/data.csv"

using namespace std;


/**
  split takes a character buf, and uses strtok to populate
  an array of token*'s. 
*/


int main(){

    bool success = createUserDb();
    createPrivilegeTable();
    createPrivilegeDb();

    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    u.addUser("a", "b");
    User u2("a", "b");
    u.assignPerm(u2, "MAIN_DB", 1);
    u.assignPerm(u2, "MAIN_DB", "MAIN_TABLE", 2);

    Database db(&u);
    db.create("MAIN_DB");
    db.create("TEMP_DB");
    db.drop();
    db.connect("MAIN_DB");

    std::string db_name = "MAIN_DB";
    vector<string> table_name = {"MAIN_TABLE", "TEMP_TABLE_2"}; 
    Table* tables[2];
    for(int i =0; i<2; i++){
        std::vector<std::pair<std::string, int> > cols;
        cols.push_back({"KEY", VARCHAR});
        cols.push_back({"VALUE", VARCHAR});  
        std::vector<int> pk = {0};

        char* table_cstr = new char[table_name[i].size()+1];
        memcpy(table_cstr, table_name[i].c_str(), table_name[i].size()+1);

        char* db_cstr = new char[db_name.size()+1];
        memcpy(db_cstr, db_name.c_str(), db_name.size()+1);

        Schema *schema = new Schema(cols, pk);
        vector<IndexData> vi;
        tables[i] = new Table(schema, table_cstr, db_cstr, false, vi);
        std::cout << "Creating table " << i << std::endl;
        db.createTable(tables[i]);
        delete table_cstr, db_cstr;
    }

    cout<<"\n----------------------------------------------------------------"<<endl;
    cout << "Table and Db Checking" << endl;
    cout << "MAIN_DB Exists? " << isDb("MAIN_DB") << endl;
    cout << "MAIN_TABLE Exists? " << isTable("MAIN_DB", "MAIN_TABLE") << endl;
    cout << "TEMP_DB Exists? " << isDb("TEMP_DB") << endl;
    cout << "TEMP_TABLE Exists? " << isTable("MAIN_DB", "TEMP_TABLE") << endl;

    Database db1(&u2);

    cout<<"\n----------------------------------------------------------------"<<endl;
    cout << "User permission validity" << endl;
    cout << "MAIN_DB Allowed to Read? " << db1.isAllowed("MAIN_DB", 1) << endl;
    cout << "MAIN_DB Allowed to Write? " << db1.isAllowed("MAIN_DB", 2) << endl;
    cout << "MAIN_TABLE Allowed to Read? " << db1.isAllowed("MAIN_DB", "MAIN_TABLE", 1) << endl;
    cout << "MAIN_TABLE Allowed to Write? " << db1.isAllowed("MAIN_DB", "MAIN_TABLE", 2) << endl;
    cout << "TEMP_TABLE_1 Allowed to Write? " << db1.isAllowed("MAIN_DB", "TEMP_TABLE_1", 2) << endl; 
    cout<<"\n----------------------------------------------------------------"<<endl;

    return 0;
}