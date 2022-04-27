#include <bits/stdc++.h>
#include <utility>
#include "schema.h"
#include "table.h"
using namespace std;

int main(){
    cout<<"Start"<<endl;
    std::vector<std::pair<std::string, int> > cols; 
    cols.push_back(make_pair("ID", INT));
    cols.push_back(make_pair("DB", INT));
    cols.push_back(make_pair("DB1", INT));
    cols.push_back(make_pair("DB2", INT));
    vector<int> pk;
    pk.push_back(0);
    pk.push_back(1);
    pk.push_back(1);
    pk.push_back(1);
    cout<<"Creating table"<<endl;
    Schema s = Schema(cols, pk);
    string x = s.encodeSchema();
    cout<<"Encoded"<<endl;
    cout<<x.size()<<' '<<x<<endl;
    Schema d = decodeSchema((char*)x.c_str(),x.size());
    cout<<"decoded"<<endl;
    d.print();  
    return 0;
}