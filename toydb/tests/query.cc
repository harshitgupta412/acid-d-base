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
#define INDEX_NAME "FIRST_DB.0.idx"
#define CSV_NAME "./dblayer/data.csv"

using namespace std;

bool all_func(RecId r, char* record, int len){
    return true;
}

bool equal_func(RecId r, char* record, int len){
    int val = DecodeInt(record);
    return val == 1;
}

int main(){
    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    Database db(&u);
    db.create(DB_NAME);

    Schema s = Schema({{"ID", INT}, {"VALUE", INT}}, {0});

    Table t(&s, "a", DB_NAME, true, {}, true);

    char **array = new char*[4];
    array[0] = "4"; array[2] = "1";
    array[1] = "2"; array[3] = "3";

    void** row1 = new void*[2];
    row1[0] = (void*)(array[0]); row1[1] = (void*)(array[1]);
    void** row2 = new void*[2];
    row2[0] = (void*)(array[2]); row2[1] = (void*)(array[3]);

    t.addRow(row1, true);
    t.addRow(row2, true);
    std::cout << "Original Table" << std::endl;
    t.print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Getting all rows with ID = 1" << std::endl;
    Table *equal_3 = t.query(equal_func);
    equal_3->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Getting all rows" << std::endl;
    Table *all = t.query(all_func);
    all->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    return 0;
}