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
    char name[] = "hapipola"; char age[] = "221001"; char salary[] = "21220";
    printf("%s\n", name);

    n[0] = name; n[1] = age; n[2] = salary;
    
    if (!c.add("creme.pie1", (void**)n))
    {
        c.rollback();
        c.endTxn();
        c.disconnect();
        exit(1);
    }
    printf("add done\n");
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