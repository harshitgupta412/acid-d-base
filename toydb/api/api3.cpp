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
    char name[] = "hello8"; char age[] = "115244"; char salary[] = "122210";
    printf("%s\n", name);

    n[0] = age;// n[1] = age; n[2] = salary;

    c.connect2mngr();
    c.initTxn(u);
    
    if(!c.del("creme.pie1", (void**)n)) {
        c.endTxn();
        c.disconnect();
        return 1;
    }
    printf("add done\n");

    // c.rollback();
    c.endTxn();

    c.disconnect();
    
}