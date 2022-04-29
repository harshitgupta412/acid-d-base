#include <vector>
#include <unordered_map>
#include <unordered_set>
#include "table.h"
#include "db.h"
#include "user.h"
#include <string>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>

/* BEGIN globals
	when the daemon is started, it should initialize these global variables
	the logic should be implemented in initilize_globals() function
	These will be used as is in transaction handling
	Currently not including table, db creation and deletion -- TODO
	implementation -- left for now -- TODO
*/

std::unordered_set<std::string> global_db;
std::unordered_map<std::string, std::unordered_set<std::string> > global_tbl;
std::unordered_map<std::string, std::unordered_map<std::string, int> > global_tbl_readers;
std::unordered_map<std::string, std::unordered_map<std::string, int> > global_tbl_writers;

std::unordered_set<std::string> global_usr;
std::unordered_map<std::string, std::unordered_map<std::string, int> > global_usr_db_perm;
std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, int> > > global_usr_tbl_perm;

void initialize_globals()
{
    User u("SUPERUSER", "SUPERUSER_PASSWORD");
    Database db(&u);
	printf("kk1");
    db.connect("DB");
	printf("kk2");

    // databases
    Table *tbl = db.load("DB_TABLE");
    std::vector< std::pair <int, void**> > dbs = tbl->get_records();
    for(int i = 0; i < dbs.size(); i++)
    {
        global_db.insert(std::string((char*)(dbs[i].second[0])));
    }
    tbl->close();

    // tables
    tbl = db.load("DB_CROSS_TABLE");
    dbs = tbl->get_records();
    for(int i = 0; i < dbs.size(); i++)
    {
        std::string db_name = std::string((char*)(dbs[i].second[0]));
        std::string tbl_name = std::string((char*)(dbs[i].second[1]));
        global_tbl[db_name].insert(tbl_name);
    }
    tbl->close();

    // users
    db.connect("DB_USER_DB");
    tbl = db.load("DB_USER_TABLE");
    dbs = tbl->get_records();
    for(int i = 0; i < dbs.size(); i++)
    {
        std::string user_name = std::string((char*)(dbs[i].second[0]));
        global_usr.insert(user_name);
    }

    // base perm
    for (auto usr : global_usr)
        for (auto db : global_db)
        {
            global_usr_db_perm[usr][db] = 0;
            for (auto tbl : global_tbl[db])
                global_usr_tbl_perm[usr][db][tbl] = 0;
        }

    // user db perm
    tbl = db.load("DB_USER_PRIV_DB");
    dbs = tbl->get_records();
    for(int i = 0; i < dbs.size(); i++)
    {
        std::string user_name = std::string((char*)(dbs[i].second[0]));
        std::string db_name = std::string((char*)(dbs[i].second[1]));
        global_usr_db_perm[user_name][db_name] = *(int*)(dbs[i].second[2]);
    }

    // user tbl perm
    tbl = db.load("DB_USER_PRIV_TABLE");
    dbs = tbl->get_records();
    for(int i = 0; i < dbs.size(); i++)
    {
        std::string user_name = std::string((char*)(dbs[i].second[0]));
        std::string db_name = std::string((char*)(dbs[i].second[1]));
        std::string tbl_name = std::string((char*)(dbs[i].second[2]));
        global_usr_tbl_perm[user_name][db_name][tbl_name] = *(int*)(dbs[i].second[3]);
    }

}

/* END globals */

/* BEGIN Transaction
	struct Transaction --  decoded from a client's request
	this should be checked to be valid and accepted/rejected according to
	R/W lock logic
	On acceptance - 
		send an OK response to client and wait on the connection for "timeout" ms
		if the client responds with an in-out list before timeout -- spawn a thread to merge the list in the db
			if the merge thread returns successfully -- send an ACCEPT response to client
			else -- send a REJECT response to client
		else -- send a REJECT response to client
		close the connection

struct Transaction
{
	char* username;
	int N;
	char** db_name;
	char** tbl_name;
	int* access_type;
	int timeout;
};

struct Transaction decode_txn(char* buffer)
{
	struct Transaction txn;
	printf("starting decode\n");
	int len = DecodeCString(buffer, &txn.username, MAX_STR_LEN);
	printf("got username -- %s\n", txn.username);
	buffer += len+4;
	txn.N = DecodeInt(buffer);
	printf("got N -- %d\n", txn.N);
	buffer += 4;
	txn.db_name = (char**)malloc(txn.N*sizeof(char*));
	txn.tbl_name = (char**)malloc(txn.N*sizeof(char*));
	txn.access_type = (int*)malloc(txn.N*sizeof(int));
	for(int i=0; i<txn.N; i++)
	{
		len = DecodeCString(buffer, &txn.db_name[i], MAX_STR_LEN);
		printf("got db_name[%d] -- %s\n", i, txn.db_name[i]);
		buffer += len+4;
		len = DecodeCString(buffer, &txn.tbl_name[i], MAX_STR_LEN);
		printf("got tbl_name[%d] -- %s\n", i, txn.tbl_name[i]);
		buffer += len+4;
		txn.access_type[i] = DecodeInt(buffer);
		printf("got access_type[%d] -- %d\n", i, txn.access_type[i]);
		buffer += 4;
	}
	txn.timeout = DecodeInt(buffer);
	printf("got timeout -- %d\n", txn.timeout);
	return txn;
}

void printTxn(struct Transaction txn)
{
	printf("Username: %s\n", txn.username);
	printf("N: %d\n", txn.N);
	for(int i=0; i<txn.N; i++)
	{
		printf("db_name: %s\t", txn.db_name[i]);
		printf("tbl_name: %s\t", txn.tbl_name[i]);
		printf("access_type: %d\n", txn.access_type[i]);
	}
	printf("timeout: %d\n", txn.timeout);
}

int validateTxn(struct Transaction txn)
{
	int user_idx = -1;
	for (int i = 0; i < global_user_count; ++i)
	{
		if(strcmp(global_user_list[i], txn.username) == 0)
		{
			user_idx = i;
			break;
		}
	}
	if(user_idx == -1)
	{
		printf("User not found\n");
		return 0;
	}

	for (int i = 0; i < txn.N; ++i)
	{
		int db_idx = -1;
		for (int j = 0; j < global_db_count; ++j)
		{
			if(strcmp(global_db_list[j], txn.db_name[i]) == 0)
			{
				db_idx = j;
				break;
			}
		}
		if(db_idx == -1)
		{
			printf("Database %d not found\n", i);
			return 0;
		}
		// TODO -- check db level permissions after adding table creation, etc here

		int tbl_idx = -1;
		for (int j = 0; j < global_tbl_count[db_idx]; ++j)
		{
			if(strcmp(global_tbl_list[db_idx][j], txn.tbl_name[i]) == 0)
			{
				tbl_idx = j;
				break;
			}
		}
		if(tbl_idx == -1)
		{
			printf("Table %d not found\n", i);
			return 0;
		}
		if(global_user_tbl_perm[user_idx][db_idx][tbl_idx] < txn.access_type[i])
		{
			printf("Access on Table %d with type %d not allowed\n", i, txn.access_type[i]);
			return 0;
		}

		if (txn.access_type[i] == 1)
		{
			if(global_tbl_write_list[db_idx][tbl_idx] == 1)
			{
				printf("Table %d is currently open in write mode, declining read request\n", i);
				return 0;
			}
		}
		else if (txn.access_type[i] == 2)
		{
			if(global_tbl_read_list[db_idx][tbl_idx] > 0)
			{
				printf("Table %d is currently open in read mode, declining write request\n", i);
				return 0;
			}
			else if (global_tbl_write_list[db_idx][tbl_idx] == 1)
			{
				printf("Table %d is currently open in write mode, declining write request\n", i);
				return 0;
			}
		}
	}

	for (int i = 0; i < txn.N; ++i)
	{
		int db_idx = -1;
		for (int j = 0; j < global_db_count; ++j)
		{
			if(strcmp(global_db_list[j], txn.db_name[i]) == 0)
			{
				db_idx = j;
				break;
			}
		}
		for (int j = 0; j < global_tbl_count[db_idx]; ++j)
		{
			if(strcmp(global_tbl_list[db_idx][j], txn.tbl_name[i]) == 0)
			{
				if(txn.access_type[i] == 2)
				{
					global_tbl_write_list[db_idx][j] = 1;
				}
				else if (txn.access_type[i] == 1)
				{
					global_tbl_read_list[db_idx][j]++;
				}
				break;
			}
		}
	}

	return 1;

}

void endTxn(struct Transaction txn)
{
	for (int i = 0; i < txn.N; ++i)
	{
		int db_idx = -1;
		for (int j = 0; j < global_db_count; ++j)
		{
			if(strcmp(global_db_list[j], txn.db_name[i]) == 0)
			{
				db_idx = j;
				break;
			}
		}
		for (int j = 0; j < global_tbl_count[db_idx]; ++j)
		{
			if(strcmp(global_tbl_list[db_idx][j], txn.tbl_name[i]) == 0)
			{
				if(txn.access_type[i] == 2)
				{
					global_tbl_write_list[db_idx][j] = 0;
				}
				else if (txn.access_type[i] == 1)
				{
					global_tbl_read_list[db_idx][j]--;
				}
				break;
			}
		}
	}
}

/* END Transaction */

int main(int argc , char *argv[])
{
    initialize_globals();

    // print everything
    for (auto db : global_db)
    {
        printf("DB %s\n", db.c_str());
        for (auto tbl : global_tbl[db])
        {
            printf("\tTBL %s\n", tbl.c_str());
        }
    }

/*
	// initialize_global_lists();

	int socket_desc , client_sock , c , read_size;
	struct sockaddr_in server , client;
	char client_message[2000];
	
	//Create socket
	socket_desc = socket(AF_INET, SOCK_STREAM, 0);
	if (socket_desc == -1)
	{
		printf("Could not create socket");
	}
	puts("Socket created");
	
	//Prepare the sockaddr_in structure
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons( 8900 );
	
	//Bind
	if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
	{
		//print the error message
		perror("bind failed. Error");
		return 1;
	}
	puts("bind done");
	
	//Listen
	listen(socket_desc , 3);
	
	//Accept and incoming connection
	puts("Waiting for incoming connections...");
	c = sizeof(struct sockaddr_in);
	
	while (1)
	{
		//accept connection from an incoming client
		client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
		if (client_sock < 0)
		{
			perror("accept failed");
			return 1;
		}
		puts("Connection accepted");
		
		//Receive a message from client
		while( (read_size = recv(client_sock , client_message , 2000 , 0)) > 0 )
		{
			printf("size - %d\n", read_size);
			struct Transaction txn = decode_txn(client_message);
			printTxn(txn);
			// send client OK
			send(client_sock , "OK" , 2 , 0 );
		}
		
		if(read_size == 0)
		{
			puts("Client disconnected");
			fflush(stdout);
		}
		else if(read_size == -1)
		{
			perror("recv failed");
		}
	}
	
	return 0;
*/
}