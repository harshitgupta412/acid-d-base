#include "client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include "../dblayer/codec.h"
#include "../db.h"
#include <unistd.h>
#include<cstring>

#define TABLEDIR ""

bool sendMsg(int sock, MsgType type, char* msg, int len) {
    char message[MAXMSGLEN];
    char* buf = message;
    buf += EncodeInt(type, buf);
    memcpy(buf, msg, len);
    if( send(sock , message , len+4 , 0) < 0)
    {
        puts("Send failed");
        return false;
    }
    return true;
}

int rcvMsg(int sock, char* msg) {
    int read_size = 0;
    if( (read_size = recv(sock, msg , MAXMSGLEN , 0)) <= 0)
    {
        if (read_size == 0) {
            puts("Server disconnected");
            return read_size;
        }
        else {
            perror("recv failed");
            return read_size;
        }
    }
    msg[read_size] = '\0';
    return read_size;
}

void Client::rollback() {
    printf("rollback\n");
    for(auto it = openTables.begin(); it != openTables.end(); it++) {
        it->second->rollback();
    }
}

bool Client::connect2mngr() {
	struct sockaddr_in server;
	sock = socket(AF_INET , SOCK_STREAM , 0);
	if (sock == -1)
	{
		fprintf(stderr, "Could not create socket");
        return false;
	}
	server.sin_addr.s_addr = inet_addr("127.0.0.1");
	server.sin_family = AF_INET;
	server.sin_port = htons( 8900 );
	//Connect to remote server
	if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
	{
		perror("connect failed. Error");
		return false;
	}

    return true;
}

bool Client::initTxn(std::string user, std::string pass) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)user.c_str(), msg, MAXMSGLEN);
    if(!sendMsg(sock, Init, msg, len))
        return false;
    else {
        int read_size = rcvMsg(sock, msg);
        txnNo = DecodeInt(msg);
        if(txnNo < 0) return false;
        this->user = User(user, pass);
        return true;
    }
}

bool Client::initTxn(User _user) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)_user.get_user().c_str(), msg, MAXMSGLEN);
    if(!sendMsg(sock, Init, msg, len))
        return false;
    else {
        int read_size = rcvMsg(sock, msg);
        txnNo = DecodeInt(msg);
        this->user = _user;
        if(txnNo < 0) return false;
        return true;
    }
}

Table* Client::evalQueryTree(QueryObj* q, User user) {
    if( q->type == Identity_) {
        std::string dbname = q->tableName.substr(0, q->tableName.find('.'));
        std::string tablename = q->tableName.substr(q->tableName.find('.')+1, q->tableName.size());
        Table* t;
        if(openTables.count(q->tableName)) {
            t = openTables[q->tableName];
        }
        else {
            Database db(&user);
            db.connect(TABLEDIR + dbname);
            t = db.load(tablename);
        }
        return t;
    }
    else if( q->type == SelectQ_) {
        Table* t = evalQueryTree(q->left, user);
        std::vector<int> cols;
        Table *t2 = t->query(q->callback);
        if(!openTables.count(t->get_name())) {
            std::cout<<"ASsa"<<std::endl;
            delete t;
        }
        return t2;
    }
    else if( q->type == SelectO_) {
        Table* t = evalQueryTree(q->left, user);
        std::vector<int> cols;
        Table *t2 = t->queryIndex(q->indexNo, q->op, q->values);
        if(!openTables.count(t->get_name()))
            delete t;
        // delete t;
        return t2;
    }
    else if( q->type == Project_) {
        Table* t = evalQueryTree(q->left, user);
        Table* t2 = t->project(q->cols1);
        if(!openTables.count(t->get_name()))
            delete t;
        // delete t;
        return t2;
    }
    else if( q->type == Join_) {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        Table* t3 = table_join(t1, t2, q->cols1, q->cols2);
        if(!openTables.count(t1->get_name()))
            delete t1;
        if(!openTables.count(t2->get_name()))
            delete t2;
        // delete t1;
        // delete t2;
        return t3;
    }
    else if( q->type == Union_) {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        Table* t3 = table_union(t1, t2);
        if(!openTables.count(t1->get_name()))
            delete t1;
        if(!openTables.count(t2->get_name()))
            delete t2;
        return t3;
    }
    else {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        Table* t3 = table_intersect(t1, t2);
        if(!openTables.count(t1->get_name()))
            delete t1;
        if(!openTables.count(t2->get_name()))
            delete t2;
        return t3;
    }
}

bool Client::evalQuery(QueryObj q, void**** result, int &len) {
    for(auto t: q.usedTables) {
        char msg[MAXMSGLEN];
        int len = EncodeCString((char*)t.c_str(), msg, MAXMSGLEN);
        if(!sendMsg(sock, Read, msg, len)) {
            rollback();
            return false;
        }
        int read_size = rcvMsg(sock, msg);
        if(read_size <= 0) {
            rollback();
            return false;
        }
        if(DecodeInt(msg) == 0) {
            fprintf(stderr, "Table permission not granted");
            rollback();
            return false;
        }
    }
    Table * t = evalQueryTree(&q, user);
    std::vector<std::pair<int,void**>> data = t->get_records2();
    *result = (void***)malloc(sizeof(void**) * data.size());
    for(int i = 0; i < data.size(); i++) {
        (*result)[i] = data[i].second;
    }
    len = data.size();
    if (!openTables.count(t->get_name())) {
        delete t;
    }
    return true;
}

bool Client::add(std::string name, void** data) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)name.c_str(), msg, MAXMSGLEN);
    if(!sendMsg(sock, Write, msg, len)) {
        fprintf(stderr, "send failed\n");
        rollback();
        return false;
    }
    int read_size = rcvMsg(sock, msg);
    if(read_size <= 0) {
        fprintf(stderr, "recv failed\n");
        rollback();
        return false;
    }
    if(DecodeInt(msg) == 0) {
        fprintf(stderr, "Table permission not granted");
        rollback();
        return false;
    }
    if(openTables.count(name) == 0) {
        printf("Table %s not open, opening\n", name.c_str());
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.')+1, name.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        openTables[name] = t;
        printf("Table %s opened\n", name.c_str());
    } 
    bool ret = openTables[name]->addRow(data, false, true);
    if(!ret) {
        std::cout<<"Failed to add row"<<std::endl;
        rollback();
        return false;
    }
    return true;
}


bool Client::update(std::string name, void** data) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)name.c_str(), msg, MAXMSGLEN);
    if(!sendMsg(sock, Write, msg, len)) {
        rollback();
        return false;
    }
    int read_size = rcvMsg(sock, msg);
    if(read_size <= 0) {
        rollback();
        return false;
    }
    if(DecodeInt(msg) == 0) {
        fprintf(stderr, "Table permission not granted");
        rollback();
        return false;
    }
    if(openTables.count(name) == 0) {
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.')+1, name.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        openTables[name] = t;
    } 
    return openTables[name]->addRow(data, true, true);
}


bool Client::del(std::string name, void** pk) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)name.c_str(), msg, MAXMSGLEN);
    if(!sendMsg(sock, Write, msg, len)) {
        rollback();
        return false;
    }
    int read_size = rcvMsg(sock, msg);
    if(read_size <= 0) {
        rollback();
        return false;
    }
    if(DecodeInt(msg) == 0) {
        fprintf(stderr, "Table permission not granted");
        rollback();
        return false;
    }
    if(openTables.count(name) == 0) {
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.')+1, name.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        openTables[name] = t;
    } 
    openTables[name]->print();
    return openTables[name]->deleteRow(pk, true);
}

bool Client::endTxn() {
    txnNo = -1;
    for(auto t: openTables) {
        t.second->close();
    }
    openTables.clear();
    bool ret = sendMsg(sock, Close, "", 0);
    sock = -1;
    return ret;
}

void Client::disconnect() {
    close(sock);
}

Client::Client(std::string user, std::string pass): user(user, pass) {
    this->user = User(user, pass);
}

Client::Client(User *user) : user(*user){
}


bool Client::createTable(std::string dbname, std::string tablename, Schema s) {
    Database db(&user);
    db.connect(TABLEDIR + dbname);
    Table* t = db.load(tablename);
    if(t != NULL) {
        return false;
    }
    t->close();
    Table t2(&s, (char*)tablename.c_str(), (char*)dbname.c_str(), false, std::vector<IndexData>());
    db.createTable(&t2);
    return true;
}

bool Client::dropTable(std::string dbname, std::string tablename) {
    Database db(&user);
    db.connect(TABLEDIR + dbname);
    Table* t = db.load(tablename);
    db.deleteTable(t);
}