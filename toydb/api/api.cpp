#include "client.h"
#include <vector>
using namespace std;

bool Tautology(int, char*, int)
{
    return true;
}

int main() {
    User u("SUPERUSER", "SUPERUSER_PASSWORD");
    Client c(&u);

    c.connect2mngr();
    c.initTxn(u);

    char* n[3];
    char name[] = "hello"; char age[] = "5"; char salary[] = "10";
    n[0] = name; n[1] = age; n[2] = salary;
    
    c.add("creme.pie1", (void**)n);
    printf("add done\n");
    QueryObj q("creme.pie1");
    q.Select(&q,Tautology);

    void **result;
    c.evalQuery(q,&result);
    c.endTxn();
    c.disconnect();
    
}