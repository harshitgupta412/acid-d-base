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
    Table* t = db.load("main");
    cout << t->get_name() << endl;
    // t->print();
    // t->print();

    Table *small = t->queryIndex(0, LESS_THAN, {(void*) "Cambodia"});

    small->print();

    Table *greater = t->queryIndex(0, GREATER_THAN_EQUAL, {(void*) "Cambodia"});

    greater->print();

    return 0;
}