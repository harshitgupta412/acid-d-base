#include <bits/stdc++.h>
#include <utility>
#include "schema.h"
#include "table.h"
using namespace std;

int main(){
    std::vector<std::pair<std::string, int> > cols; 
    cols.push_back(make_pair("ID", 0));
    vector<int> pk;
    pk.push_back(0);

    Schema s = Schema(cols, pk);
    char* x = s.encodeSchema();
    // Schema d = decodeSchema(x,1000);

    // vector<int> v = d.getpk();
    // for (int x: v){
    //     cout << x << endl;
    // }
    return 0;
}