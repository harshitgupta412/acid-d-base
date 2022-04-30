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
    std::cout << "Creating index on Primary Key" << std::endl;
    t.createIndex({0});
    std::cout<<"----------------------------------------------------------------"<<std::endl;
    int i = 3;
    std::cout << "Querying for rows with ID < 3 using index" << std::endl;
    Table *small = t.queryIndex(0, LESS_THAN, {(void*) &i});
    small->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    Table *greater = t.queryIndex(0, GREATER_THAN, {(void*) &i});
    std::cout<<"Querying for rows with ID > 3 using index" << std::endl;
    greater->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Deleting index on Primary Key" << std::endl;
    t.eraseIndex({0});
    std::cout<<"----------------------------------------------------------------"<<std::endl;

    std::cout << "Negative test for query index" << std::endl;
    Table *greater_new = t.queryIndex(0, GREATER_THAN, {(void*) &i});
    greater_new->print();
    std::cout<<"----------------------------------------------------------------"<<std::endl;
    return 0;
}