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

#define DB_NAME "SCUSTOM_SECOND_DB"
#define INDEX_NAME "FIRST_DB.0.idx"
#define CSV_NAME "./dblayer/data.csv"

using namespace std; 

int main(){
    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    Database db(&u);
    db.connect(DB_NAME);

    Schema s = Schema({{"ID", VARCHAR}, {"VALUE", VARCHAR}}, {0});

    Table t(&s, "a", DB_NAME, true, {}, false);

    char **array = new char*[4];
    array[0] = "4"; array[2] = "1";
    array[1] = "2"; array[3] = "3";

    void** row1 = new void*[2];
    row1[0] = (void*)(array[0]); row1[1] = (void*)(array[1]);
    void** row2 = new void*[2];
    row2[0] = (void*)(array[2]); row2[1] = (void*)(array[3]);

    t.addRow(row1, true);
    t.addRow(row2, true);
    db.createTable(&t);

    db.print();


    Table t2(&s, "b", DB_NAME, true, {}, false);

    char **array2 = new char*[4];
    array2[0] = "6";  array2[2] = "1";
    array2[1] = "4"; array2[3] = "5";

    void** row3 = new void*[2];
    row3[0] = (void*)(array2[0]); row3[1] = (void*)(array2[1]);
    void** row4 = new void*[2];
    row4[0] = (void*)(array2[2]); row4[1] = (void*)(array2[3]);

    t2.addRow(row3, true);
    t2.addRow(row4, true);

    db.createTable(&t2);

    db.print();

    db.deleteTable(&t);

    db.print();
    return 0;
}