#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include <stdio.h>
#include <stdlib.h>
#ifdef __cplusplus /* If this is a C++ compiler, use C linkage */
extern "C" {
#endif

#include "./dblayer/tbl.h"

#ifdef __cplusplus /* If this is a C++ compiler, end C linkage */
}
#endif
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <map>

class Table;

class Schema {
    Schema_ *schema;
    std::vector<int> pk;
    

    public:
    std::map<std::vector<int>, std::string> fk;
    Schema(std::vector<std::pair<std::string, int> > cols, std::vector<int> _pk);

    Schema(Schema_ *sch, std::vector<int> _pk);

    bool foreignKey(std::vector<int> ref_cols, char* ref_tbl);

    const Schema_* getSchema();
    

    std::vector<int> getpk();

    std::string encodeSchema();

    void print();
};

Schema decodeSchema(char * s, int max_len, int* length = NULL);

#endif
