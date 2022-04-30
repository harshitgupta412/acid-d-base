#include <bits/stdc++.h>
#include <utility>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "schema.h"
#include "table.h"
#include "db.h"
#include "user.h"

int main(){

    // ONLY RUN THESE THE FIRST TIME TO CREATE SUPERUSER
    // bool success = createUserDb();
    // createPrivilegeTable();
    // createPrivilegeDb();

    User u("SUPERUSER", "SUPERUSER_PASSWORD");

    // u.addUser("weakling", "boi");
    // User u2("weakling", "boi");
    // u.assignPerm(u2, "MAIN_DB", 1);
    // u.assignPerm(u2, "MAIN_DB", "MAIN_TABLE", 2);

    Database db(&u);
    // db.create("MAIN_DB");
    // db.create("TEMP_DB");
    // db.drop();
    db.create("creme");
    Schema_ schema;
    schema.numColumns = 3;
    schema.columns = new ColumnDesc[3];
    schema.columns[0].name = "name";
    schema.columns[0].type = VARCHAR;
    schema.columns[1].name = "age";
    schema.columns[1].type = INT;
    schema.columns[2].name = "sex";
    schema.columns[2].type = INT;
    Table tbl1 (new Schema(&schema, {1}), "pie1", "creme", true);
    Table tbl2 (new Schema(&schema, {2}), "pie2", "creme", true);
    
    std::vector <std::string> name1 = {"John", "Jane", "Joe"};
    std::vector <std::string> age1 = {"20", "30", "40"};
    std::vector <std::string> sex1 = {"0", "1", "0"};
    std::vector <std::string> name2 = {"John", "Jane", "Joe"};
    std::vector <std::string> age2 = {"20", "30", "40"};
    std::vector <std::string> sex2 = {"0", "1", "0"};

    db.createTable(&tbl1);
    db.createTable(&tbl2);

    for (int i = 0; i < 3; i++){
        void **data = new void*[3];
        data[0] = (void*)name1[i].c_str(); data[1] = (void*)age1[i].c_str(); data[2] = (void*)sex1[i].c_str();
        tbl1.addRow(data, true);
        data[0] = (void*)name2[i].c_str(); data[1] = (void*)age2[i].c_str(); data[2] = (void*)sex2[i].c_str();
        tbl2.addRow(data, true);
        free(data);
    }

    tbl1.print();
    tbl2.print();

    table_intersect(&tbl1, &tbl2)->print();
    table_union(&tbl1, &tbl2)->print();
    table_join(&tbl1, &tbl2, {1}, {1})->print();

    tbl1.close();
    tbl2.close();

    return 0;
}