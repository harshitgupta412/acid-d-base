#ifndef _QUERYOBJ_H_
#define _QUERYOBJ_H_

#define TABLEDIR "../"
#define MAXMSGLEN 1024

#include<string>
#include<vector>
#include<set>
#include "queryobj.h"
#include "../user.h"
#include "../db.h"
#include <map>

enum MsgType {
    Init,
    Read,
    Write,
    Close
};

class Client {
    int txnNo;
    std::map<std::string, Table*> openTables;
    User user;
    int sock;
    Table* evalQueryTree(QueryObj* q, User user);
    
    public:
    Client(std::string user, std::string pass);
    Client(User *user);
    bool connect2mngr();
    bool initTxn(std::string user, std::string pass);
    bool initTxn(User _user);
    bool evalQuery(QueryObj q, void**** result, int& len);
    bool add(std::string name, void** data);
    bool update(std::string name, void** data);
    bool del(std::string name, void** pk);
    bool endTxn();
    void disconnect();
    void rollback();

    bool createTable(std::string dbname, std::string tablename, Schema s);
    bool dropTable(std::string dbname, std::string tablename);
};

#endif
