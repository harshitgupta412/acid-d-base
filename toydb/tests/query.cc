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

bool all_func(RecId r, char* record, int len){
    return true;
}

bool yemen_func(RecId r, char* record, int len){
    short string_len = DecodeShort(record);
    char* str = (char *)malloc(string_len+1);
    int strlen = DecodeCString(record, str, len);


    if (strcmp(str, "Yemen") == 0){
        free(str);
        return true;
    }
    return false;
}

int main(){
    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    Database db(&u);
    db.connect(DB_NAME);
    Table* t = db.load("main");
    cout << t->get_name() << endl;
    t->print();
    // t->print();

    Table *yemen = t->query(yemen_func);

    yemen->print();

    Table *all = t->query(all_func);

    all->print();

    return 0;
}