#include "client.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include "../dblayer/codec.h"
#include "../db.h"
#include <unistd.h>
#include<cstring>

#define TABLEDIR "../"

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

Table* evalQueryTree(QueryObj* q, User user) {
    if( q->type == Identity_) {
        std::string dbname = q->tableName.substr(0, q->tableName.find('.'));
        std::string tablename = q->tableName.substr(q->tableName.find('.'), q->tableName.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        return t;
    }
    else if( q->type == SelectQ_) {
        Table* t = evalQueryTree(q->left, user);
        std::vector<int> cols;
        Table *t2 = t->query(q->callback);
        delete t;
        return t2;
    }
    else if( q->type == SelectO_) {
        Table* t = evalQueryTree(q->left, user);
        std::vector<int> cols;
        Table *t2 = t->queryIndex(q->indexNo, q->op, q->values);
        delete t;
        return t2;
    }
    else if( q->type == Project_) {
        Table* t = evalQueryTree(q->left, user);
        // Table* t2 = Table_Project(t, q->cols1);
        // delete t;
        // return t2;
        return t;
    }
    else if( q->type == Join_) {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        Table* t3 = table_join(t1, t2, q->cols1, q->cols2);
        delete t1;
        delete t2;
        return t3;
    }
    else if( q->type == Union_) {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        // Table* t3 = Table_Union(t1, t2);
        delete t1;
        delete t2;
        // return t3;
        return t2;
    }
    else {
        Table* t1 = evalQueryTree(q->left, user);
        Table* t2 = evalQueryTree(q->right, user);
        // Table* t3 = Table_Intersection(t1, t2);
        delete t1;
        delete t2;
        // return t3;
        return t2;
    }
}

bool Client::evalQuery(QueryObj q, void*** result) {
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
    std::vector<std::pair<int,void**>> data = t->get_records();
    result = (void***)malloc(sizeof(void**) * data.size());
    for(int i = 0; i < data.size(); i++) {
        result[i] = data[i].second;
    }
    delete t;
    return true;
}

bool Client::add(std::string name, void** data) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)name.c_str(), msg, MAXMSGLEN);
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
    if(openTables.count(name) == 0) {
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.'), name.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        openTables[name] = t;
    } 
    return openTables[name]->addRow(data, false, true);
}


bool Client::update(std::string name, void** data) {
    char msg[MAXMSGLEN];
    int len = EncodeCString((char*)name.c_str(), msg, MAXMSGLEN);
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
    if(openTables.count(name) == 0) {
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.'), name.size());
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
    if(openTables.count(name) == 0) {
        std::string dbname = name.substr(0, name.find('.'));
        std::string tablename = name.substr(name.find('.'), name.size());
        Database db(&user);
        db.connect(TABLEDIR + dbname);
        Table* t = db.load(tablename);
        openTables[name] = t;
    } 
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