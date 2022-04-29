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
    
    Schema sc1 = Schema({{"ID1", VARCHAR}, {"ID2", VARCHAR}, {"ID3", INT}}, {0,2});
    Table* t1 = new Table(&sc1, "TABLE_1", (char*)DB_NAME, true, {}, false);

    std::vector<std::tuple<std::string, std::string, int> > rows;
    rows.push_back({"Hello", "hello", 1});
    rows.push_back({"Dunno", "dunno", 2});
    rows.push_back({"Bye", "bye", 3});
    rows.push_back({"Hi","hi", 4});

    for(int i = 0; i < 4; i ++) {
        void* data[3];
        data[0] = (void*)get<0>(rows[i]).c_str();
        data[1] = (void*)get<1>(rows[i]).c_str();
        data[2] = (void*)std::to_string(get<2>(rows[i])).c_str();
        t1->addRow(data, false, false);
    }

    db.createTable(t1);

    std::cout << "Table 1" << std::endl;
    t1->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Union Result" << std::endl;
    Table* result = t1->project({0,2});
    result->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    return 0;
}