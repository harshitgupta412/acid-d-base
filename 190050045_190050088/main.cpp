#include <bits/stdc++.h>
#include <utility>
#include "schema.h"
#include "table.h"
using namespace std;

int main(){
    cout<<"Start"<<endl;
    std::vector<std::pair<std::string, int> > cols; 
    cols.push_back(make_pair("ID", 0));
    cols.push_back(make_pair("DB", 1));
    vector<int> pk;
    pk.push_back(0);
    pk.push_back(1);
    cout<<"Creating table"<<endl;
    Schema s = Schema(cols, pk);
    string x = s.encodeSchema();
    cout<<"Encoded"<<endl;
    cout<<x.size()<<' '<<x<<endl;
    Schema d = decodeSchema((char*)x.c_str(),x.size());
    cout<<"decoded"<<endl;
    vector<int> v = d.getpk();
    for (int x: v){
        cout << x << endl;
    }
    Schema_ s_ = d.getSchema();
    cout<<s_.numColumns<<endl;
    for(int i=0; i< s_.numColumns; i++) {
        cout<<s_.columns[i].name<<' '<<s_.columns[i].type<<endl;
    }
    return 0;
}