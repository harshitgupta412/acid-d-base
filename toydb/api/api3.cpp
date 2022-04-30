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

    char* n[1];
    char age[] = "221001";

    n[0] = age; // n[1] = age; n[2] = salary;

    c.connect2mngr();
    c.initTxn(u);
    
    if(!c.del("creme.pie1", (void**)n)) {
        c.endTxn();
        c.disconnect();
        return 1;
    }
    printf("delete done\n");

    QueryObj q("creme.pie1");
    
    QueryObj q2 = q.Select(Tautology);

    void ***result;
    int len;
    if (!c.evalQuery(q2,&result, len))
    {
        c.rollback();
        c.endTxn();
        c.disconnect();
        exit(1);
    }

    for(int j=0;j<len;j++) {
        for(int i = 0; i<3;i++) 
            printf("%s\t",(char*)result[j][i]);
        printf("\n");
    }
    printf("eval done\n");

    c.endTxn();

    c.disconnect();
    
}