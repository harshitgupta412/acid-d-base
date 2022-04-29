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

#define DB_NAME "TEST_DB"

using namespace std;

int main(){
    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    Database db(&u);
    db.create(DB_NAME);
    
    std::vector<std::pair<std::string, int> > cols;
    cols.push_back({"ID1", VARCHAR});
    cols.push_back({"ID2", INT});  
    std::vector<int> pk = {0, 1};
    Schema sc1 = Schema(cols, pk);
    Table* t1 = new Table(&sc1, "TABLE_1", (char*)DB_NAME, false, {}, false);
    Table* t2 = new Table(&sc1, "TABLE_2", (char*)DB_NAME, false, {}, false);

    std::vector<std::pair<std::string, int> > rows;
    rows.push_back({"Hello", 1});
    rows.push_back({"Dunno", 2});
    rows.push_back({"Bye", 3});
    rows.push_back({"Hi", 4});

    for(int i = 0; i < 3; i ++) {
        void* data[2];
        data[0] = (void*)rows[i].first.c_str();
        data[1] = (void*)std::to_string(rows[i].second).c_str();
        t1->addRow(data, false, false);
    }

    for(int i = 1; i < 4; i ++) {
        void* data[2];
        data[0] = (void*)rows[i].first.c_str();
        data[1] = (void*)std::to_string(rows[i].second).c_str();
        t2->addRow(data, false, false);
    }

    db.createTable(t1);
    db.createTable(t2);

    std::cout << "Table 1" << std::endl;
    t1->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Table 2" << std::endl;
    t2->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Union Result" << std::endl;
    Table* result = table_union(t1, t2);
    result->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    return 0;
}