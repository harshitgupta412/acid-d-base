#include "client.h"
#include <unistd.h>
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
    char name[] = "hello8a"; char age[] = "10001"; char salary[] = "23";

    n[0] = name; n[1] = age; n[2] = salary;

    c.connect2mngr();
    c.initTxn(u);
    
    if (!c.add("creme.pie1", (void**)n))
    {
        c.rollback();
        c.endTxn();
        c.disconnect();
        exit(1);
    }
    sleep(10);

    printf("add done\n");

    c.rollback();
    c.endTxn();

    c.disconnect();
    
}