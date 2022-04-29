#include "client.h"
#include <vector>
using namespace std;

int main() {
    User u("SUPERUSER", "SUPERUSER_PASSWORD");
    Database db(&u);
    if(!db.create("test")){
        cout<<"hag diya gandu"<<endl;
        exit(1);    
    }
    
    vector<pair<string, int> > v(3);
    v[0] = make_pair("name", VARCHAR);
    v[1] = make_pair("age", INT);
    v[2] = make_pair("salary", INT);

    Client c(&u);
    Schema s(v, vector<int>(1, 1));
    c.createTable("test", "test", s);

    c.connect2mngr();
    c.initTxn(u);

    char* n[3];
    char name[] = "hello"; char age[] = "5"; char salary[] = "10";
    n[0] = name; n[1] = age; n[2] = salary;
    
    c.add("test.test", (void**)n);

    c.endTxn();
    c.disconnect();
}