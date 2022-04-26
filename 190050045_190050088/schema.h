#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include "./dblayer/tbl.h"
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <map>
#include "table.h"

class Schema {
    Schema_ *schema;
    std::vector<int> pk;
    std::map<std::vector<int>, Table*> fk;

    public:
    Schema(std::vector<std::pair<std::string, int>> cols, std::vector<int> _pk) {
        schema = new Schema_;
        schema->numColumns = cols.size();
        schema->columns = new ColumnDesc[schema->numColumns];
        for(int i=0; i<schema->numColumns;i++) {
            schema->columns[i].name = (char*)cols[i].first.c_str();
            schema->columns[i].type = cols[i].second;
        }
        pk = _pk;
    }

    bool foreignKey(std::vector<int> ref_cols, Table* refT) {
        fk.insert(std::pair<std::vector<int>, Table*>(ref_cols, refT));
    }

    Schema_ getSchema() {
        return *schema;
    }

    std::vector<int> getpk() {
        return pk;
    }
};

#endif
