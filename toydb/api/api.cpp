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
    char name[] = "hello3"; char age[] = "151"; char salary[] = "1220";
    printf("%s\n", name);

    n[0] = name; n[1] = age; n[2] = salary;
    
    // c.add("creme.pie1", (void**)n);
    printf("add done\n");
    QueryObj q("creme.pie1");
    
    QueryObj q2 = q.Select(Tautology);

    void ***result;
    int len;
    c.evalQuery(q,&result, len);
    for(int j=0;j<len;j++) {
        for(int i = 0; i<3;i++) 
            printf("%s\t",(char*)result[j][i]);
        printf("\n");
    }
    printf("eval done\n");
    c.endTxn();
    c.disconnect();
}