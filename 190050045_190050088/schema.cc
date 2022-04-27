#include <stdio.h>
#include <string.h>
#include "schema.h" 
#include <iostream>
using namespace std;

string char2string(char* x, int size) {
    return string(x, size);
}

Schema::Schema(std::vector<std::pair<std::string, int> > cols, std::vector<int> _pk) {
    schema = new Schema_;
    schema->numColumns = cols.size();
    schema->columns = new ColumnDesc[schema->numColumns];
    for(int i=0; i<schema->numColumns;i++) {
        schema->columns[i].name = new char[cols[i].first.length() + 1];
        strcpy(schema->columns[i].name, cols[i].first.c_str());
        schema->columns[i].type = cols[i].second;
    }
    pk = _pk;
    pk.insert(pk.begin(),0);
}

Schema::Schema(Schema_ *sch, std::vector<int> _pk) {
    schema = new Schema_;
    schema->columns = sch->columns;
    schema->numColumns = sch->numColumns;
    pk = _pk;
}

Schema decodeSchema(char * s, int max_len, int* length) {
    char* old_s = s;
    int len = DecodeInt(s);
    s += 4;
    Schema_ sch;
    sch.numColumns = len;
    sch.columns = new ColumnDesc[sch.numColumns];
    for(int i=0; i<sch.numColumns; i++) {
        int strlen = DecodeCString2(s, &sch.columns[i].name, max_len);
        s += strlen+2;
        // cout<<"columns"<<' '<<strlen<<' '<<s-strlen<<' '<<sch.columns[i].name[0]<<endl;
        sch.columns[i].type = DecodeShort(s);
        s += 2;
        // cout<<sch.columns[i].name<<' '<<sch.columns[i].type<< endl; 
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
        char * ref_tbl;
        int strlen = DecodeCString2(s, &ref_tbl, max_len);
        s += strlen;
        schema.fk.insert(std::pair<std::vector<int>, std::string>(ref_cols, ref_tbl));
        free(ref_tbl);
    }

    if(length != NULL)  
        *length = s - old_s;
    return schema;
}

bool Schema::foreignKey(std::vector<int> ref_cols, char* refT) {
    if(fk.count(ref_cols)) return false;
    fk.insert(std::pair<std::vector<int>, std::string>(ref_cols, refT));
    return true;
}

const Schema_* Schema::getSchema() {
    return schema;
}

std::vector<int> Schema::getpk() {
    return pk;
}

string Schema::encodeSchema() {
    std::string encoded = "";
    char* r = (char*) malloc(sizeof(int));
    EncodeInt(schema->numColumns, r);
    encoded.append(r, 4);
    free(r);
    for(int i=0; i<schema->numColumns; i++) {
        int len = 2+strlen(schema->columns[i].name);
        char* r = (char*)malloc(len*sizeof(char));
        len = EncodeCString(schema->columns[i].name, r, len);
        encoded.append(r, len);
        EncodeShort(schema->columns[i].type, r);
        encoded.append(r, 2);
        free(r);
    }
    r = (char*) malloc(sizeof(int));
    EncodeInt(pk.size(), r);
    encoded.append(r,4);
    free(r);
    for(int i=0; i<pk.size(); i++) {
        char* r = (char*) malloc(sizeof(int));
        EncodeInt(pk[i], r);
        encoded.append(r,4);
        free(r);
    }
    r = (char*) malloc(sizeof(int));
    EncodeInt(fk.size(), r);
    encoded.append(r,4);
    free(r);
    for(auto i: fk) {
        char* r = (char*) malloc(sizeof(int));
        EncodeInt(i.first.size(), r);
        encoded.append(r,4);
        free(r);
        for(int j=0; j<i.first.size(); j++) {
            r = (char*) malloc(sizeof(int));
            EncodeInt(i.first[j], r);
            encoded.append(r,4);
            free(r);
        }
        r = (char*) malloc(i.second.size()+2);
        // fix this
        int len = EncodeCString((char*)i.second.c_str(), r, i.second.size()+2);
        encoded.append(r, len);
        free(r);
    }
    // cout<<encoded<<endl;
    return encoded;
}  

void Schema::print() {
    std::cout << "Number of Columns: " << schema->numColumns << std::endl;
    for (int i = 0; i < schema->numColumns; i++) 
        std::cout << schema->columns[i].name << " " << schema->columns[i].type << " " << pk[i] << std::endl;
}
