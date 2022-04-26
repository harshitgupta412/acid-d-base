#include <stdio.h>
#include <string.h>
#include "schema.h" 
#include <iostream>
using namespace std;

Schema::Schema(std::vector<std::pair<std::string, int> > cols, std::vector<int> _pk) {
    schema = new Schema_;
    schema->numColumns = cols.size();
    schema->columns = new ColumnDesc[schema->numColumns];
    for(int i=0; i<schema->numColumns;i++) {
        schema->columns[i].name = (char*)cols[i].first.c_str();
        schema->columns[i].type = cols[i].second;
    }
    pk = _pk;
}

Schema::Schema(Schema_ *sch, std::vector<int> _pk) {
    schema = sch;
    pk = _pk;
}

Schema decodeSchema(char * &s, int max_len) {
    int len = DecodeInt(s);
    s += 4;
    Schema_ sch;
    sch.numColumns = len;
    sch.columns = new ColumnDesc[sch.numColumns];
    for(int i=0; i<sch.numColumns; i++) {
        int strlen = DecodeCString2(s, sch.columns[i].name, max_len);
        s += strlen;
        sch.columns[i].type = DecodeShort(s);
        s += 2;
    }
    len = DecodeInt(s);
    s += 4;
    std::vector<int> pk(len);
    for(int i=0; i<len; i++) {
        pk[i] = DecodeInt(s);
        s += 4;
    }
    Schema schema = Schema(&sch, pk);
    len = DecodeInt(s);
    s += 4;
    std::map<std::vector<int>, Table*> fk;
    for(int i=0; i<len; i++) {
        int len = DecodeInt(s);
        s += 4;
        std::vector<int> ref_cols(len);
        for(int j=0; j<len; j++) {
            ref_cols[i] = DecodeInt(s);
            s += 4;
        }
        Table* ref_tbl = (Table*) DecodeInt(s);
        s += 4;
        schema.fk.insert(std::pair<std::vector<int>, Table*>(ref_cols, (Table*)ref_tbl));
    }
}

bool Schema::foreignKey(std::vector<int> ref_cols, Table* refT) {
    fk.insert(std::pair<std::vector<int>, Table*>(ref_cols, refT));
}

Schema_ Schema::getSchema() {
    return *schema;
}

std::vector<int> Schema::getpk() {
    return pk;
}

char* Schema::encodeSchema() {
    std::string encoded = "";
    char* r = (char*) malloc(sizeof(int));
    EncodeInt(schema->numColumns, r);
    encoded += r;
    free(r);
    for(int i=0; i<schema->numColumns; i++) {
        int len = 4+strlen(schema->columns[i].name);
        char* r = (char*)malloc(len*sizeof(char));
        len = EncodeCString(schema->columns[i].name, r, len);
        EncodeShort(schema->columns[i].type, r+len);
        encoded += r;
        free(r);
    }
    r = (char*) malloc(sizeof(int));
    EncodeInt(pk.size(), r);
    encoded += r;
    free(r);
    for(int i=0; i<pk.size(); i++) {
        char* r = (char*) malloc(sizeof(int));
        EncodeInt(pk[i], r);
        encoded += r;
        free(r);
    }
    r = (char*) malloc(sizeof(int));
    EncodeInt(fk.size(), r);
    encoded += r;
    free(r);
    for(auto i: fk) {
        char* r = (char*) malloc(sizeof(int));
        EncodeInt(i.first.size(), r);
        encoded += r;
        free(r);
        for(int j=0; j<i.first.size(); j++) {
            r = (char*) malloc(sizeof(int));
            EncodeInt(i.first[j], r);
            encoded += r;
            free(r);
        }
        r = (char*) malloc(sizeof(int));
        // fix this
        EncodeInt((int)(size_t)i.second, r);
        encoded += r;
    }
    return (char*)encoded.c_str();
}   
