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

    char* n[3];
    char name[] = "hello8"; char age[] = "15244"; char salary[] = "12210";
    printf("%s\n", name);

    n[0] = name; n[1] = age; n[2] = salary;

    c.connect2mngr();
    c.initTxn(u);
    
    c.add("creme.pie1", (void**)n);
    printf("add done\n");

    c.rollback();
    c.endTxn();

    c.disconnect();
    
}