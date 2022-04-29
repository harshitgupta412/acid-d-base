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
#include <sys/time.h>
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
std::unordered_map<std::string, std::unordered_set<std::string>> global_tbl;
std::unordered_map<std::string, std::unordered_map<std::string, int>> global_tbl_readers;
std::unordered_map<std::string, std::unordered_map<std::string, int>> global_tbl_writers;

std::unordered_set<std::string> global_usr;
std::unordered_map<std::string, std::unordered_map<std::string, int>> global_usr_db_perm;
std::unordered_map<std::string, std::unordered_map<std::string, std::unordered_map<std::string, int>>> global_usr_tbl_perm;

void initialize_globals()
{
	User u("SUPERUSER", "SUPERUSER_PASSWORD");
	Database db(&u);
	db.connect("DB");

	// databases
	Table *tbl = db.load("DB_TABLE");
	std::vector<std::pair<int, void **>> dbs = tbl->get_records();
	for (int i = 0; i < dbs.size(); i++)
	{
		global_db.insert(std::string((char *)(dbs[i].second[0])));
	}
	tbl->close();

	// tables
	tbl = db.load("DB_CROSS_TABLE");
	dbs = tbl->get_records();
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string db_name = std::string((char *)(dbs[i].second[0]));
		std::string tbl_name = std::string((char *)(dbs[i].second[1]));
		global_tbl[db_name].insert(tbl_name);
	}
	tbl->close();

	// users
	db.connect("DB_USER_DB");
	tbl = db.load("DB_USER_TABLE");
	dbs = tbl->get_records();
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string user_name = std::string((char *)(dbs[i].second[0]));
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
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string user_name = std::string((char *)(dbs[i].second[0]));
		std::string db_name = std::string((char *)(dbs[i].second[1]));
		global_usr_db_perm[user_name][db_name] = *(int *)(dbs[i].second[2]);
	}

	// user tbl perm
	tbl = db.load("DB_USER_PRIV_TABLE");
	dbs = tbl->get_records();
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string user_name = std::string((char *)(dbs[i].second[0]));
		std::string db_name = std::string((char *)(dbs[i].second[1]));
		std::string tbl_name = std::string((char *)(dbs[i].second[2]));
		global_usr_tbl_perm[user_name][db_name][tbl_name] = *(int *)(dbs[i].second[3]);
	}
}

/* END globals */

/* BEGIN Transaction
	struct Transaction --  decoded from a client's request
	this should be checked to be valid and accepted/rejected according to
	R/W lock logic
*/

class Transaction
{
public:
	std::string username;
	std::vector<std::string> readTables;
	std::vector<std::string> writeTables;
};

int txnID = 0;
std::unordered_map<int, Transaction *> txnMap;
std::unordered_map<int, int> sockFD_txn;

std::string concat_db_table(std::string db, std::string tbl)
{
	return db + "." + tbl;
}

std::pair<std::string, std::string> deconcat_db_table(std::string db_tbl)
{
	int pos = db_tbl.find(".");
	return std::make_pair(db_tbl.substr(0, pos), db_tbl.substr(pos + 1));
}

std::pair<int, int> decode_lock_request(char *buffer, int sockfd)
{
	printf("Starting Decode\n");
	int type = DecodeInt(buffer);
	std::string dbname;
	std::string tablename;
	char *db_table_name;
	char *username;
	switch (type)
	{
	// transaction request -- client sends username
	case 0:
		printf("Transaction Request\n");
		if (sockFD_txn.find(sockfd) != sockFD_txn.end())
		{
			printf("Transaction %d already active on socket %d\n", sockFD_txn[sockfd], sockfd);
			return std::make_pair(0, -1);
		}
		DecodeCString2(buffer + 2, &username, 50);
		printf("Username: %s\n", username);
		txnID++;
		txnMap[txnID] = new Transaction();
		txnMap[txnID]->username = std::string(username);
		free(username);
		return std::make_pair(0, txnID);

	// read request
	case 1:
		printf("Read Lock Request\n");
		DecodeCString2(buffer + 2, &db_table_name, 50);
		txnMap[sockFD_txn[sockfd]]->readTables.push_back(std::string(db_table_name));
		dbname = deconcat_db_table(std::string(db_table_name)).first;
		tablename = deconcat_db_table(std::string(db_table_name)).second;
		printf("DB: %s, Table: %s\n", dbname.c_str(), tablename.c_str());
		if (global_usr_tbl_perm[txnMap[sockFD_txn[sockfd]]->username][dbname][tablename] < 1)
		{
			printf("User %s does not have read permission on table %s.%s\n", txnMap[sockFD_txn[sockfd]]->username.c_str(), dbname.c_str(), tablename.c_str());
			return std::make_pair(1, 0);
		}
		if (global_tbl_writers[dbname][tablename] != 0)
		{
			printf("Table %s.%s is currently being written to by another transaction\n", dbname.c_str(), tablename.c_str());
			return std::make_pair(1, 0);
		}
		global_tbl_readers[dbname][tablename]++;
		txnMap[sockFD_txn[sockfd]]->readTables.push_back(std::string(db_table_name));
		free(db_table_name);
		return std::make_pair(1, 1);

	// write request
	case 2:
		printf("Write Lock Request\n");
		DecodeCString2(buffer + 2, &db_table_name, 50);
		dbname = deconcat_db_table(std::string(db_table_name)).first;
		tablename = deconcat_db_table(std::string(db_table_name)).second;
		printf("DB: %s, Table: %s\n", dbname.c_str(), tablename.c_str());
		if (global_usr_tbl_perm[txnMap[sockFD_txn[sockfd]]->username][dbname][tablename] < 2)
		{
			printf("User %s does not have write permission on table %s.%s\n", txnMap[sockFD_txn[sockfd]]->username.c_str(), dbname.c_str(), tablename.c_str());
			return std::make_pair(1, 0);
		}
		if (global_tbl_writers[dbname][tablename] != 0)
		{
			printf("Table %s.%s is currently being written to by another transaction\n", dbname.c_str(), tablename.c_str());
			return std::make_pair(1, 0);
		}
		if (global_tbl_readers[dbname][tablename] != 0)
		{
			printf("Table %s.%s is currently being read from by another transaction\n", dbname.c_str(), tablename.c_str());
			return std::make_pair(1, 0);
		}
		global_tbl_writers[dbname][tablename] = 1;
		txnMap[sockFD_txn[sockfd]]->writeTables.push_back(std::string(db_table_name));
		free(db_table_name);
		return std::make_pair(1, 1);

	// end txn request
	case 3:
		printf("End Transaction Request\n");
		for (auto rtbl : txnMap[sockFD_txn[sockfd]]->readTables)
		{
			dbname = deconcat_db_table(std::string(rtbl)).first;
			tablename = deconcat_db_table(std::string(rtbl)).second;
			global_tbl_readers[dbname][tablename]--;
		}
		for (auto wtbl : txnMap[sockFD_txn[sockfd]]->writeTables)
		{
			dbname = deconcat_db_table(std::string(wtbl)).first;
			tablename = deconcat_db_table(std::string(wtbl)).second;
			global_tbl_writers[dbname][tablename] = 0;
		}
		delete txnMap[sockFD_txn[sockfd]];
		txnMap.erase(sockFD_txn[sockfd]);
		sockFD_txn.erase(sockfd);
		return std::make_pair(3, 1);

	default:
		printf("Invalid Request\n");
		return std::make_pair(-1, -1);
	}
}

void printTxn(Transaction txn)
{
	printf("Username: %s\n", txn.username.c_str());
	printf("Read Tables: ");
	for (auto tbl : txn.readTables)
		printf("%s ", tbl.c_str());
	printf("\n");
	printf("Write Tables: ");
	for (auto tbl : txn.writeTables)
		printf("%s ", tbl.c_str());
}

/* END Transaction */

int main(int argc, char *argv[])
{
	// initialize_globals();

	int master_sock, c, read_size;
	std::vector<int> client_sock;
	struct sockaddr_in server, client;
	char client_message[1024];
	char server_message[1024];

	// Create socket
	master_sock = socket(AF_INET, SOCK_STREAM, 0);
	if (master_sock == 0)
	{
		fprintf(stderr, "Could not create master socket");
		exit(1);
	}
	puts("Socket created");

	// Prepare the sockaddr_in structure
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(8900);

	// Bind
	if (bind(master_sock, (struct sockaddr *)&server, sizeof(server)) < 0)
	{
		// print the error message
		perror("bind failed. Error");
		return 1;
	}
	puts("bind done");

	// Listen
	if (listen(master_sock, 3) < 0)
	{
		perror("listen failed. Error");
		return 1;
	}

	// Accept and incoming connection
	puts("Waiting for incoming connections...");
	c = sizeof(struct sockaddr_in);
	fd_set readfds;
	int max_sd;
	// clear the socket set
	FD_ZERO(&readfds);
	// add master socket to set
	FD_SET(master_sock, &readfds);
	max_sd = master_sock;

	while (1)
	{
		// add child sockets to set
		for (int i = 0; i < client_sock.size(); i++)
		{
			// socket descriptor
			int sd = client_sock[i];

			// if valid socket descriptor then add to read list
			if (sd > 0)
				FD_SET(sd, &readfds);

			// highest file descriptor number, need it for the select function
			if (sd > max_sd)
				max_sd = sd;
		}

		// wait for an activity on one of the sockets , timeout is NULL ,
		// so wait indefinitely
		int activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);

		if ((activity < 0) && (errno != EINTR))
		{
			printf("select error");
		}

		// If something happened on the master socket ,
		// then its an incoming connection
		if (FD_ISSET(master_sock, &readfds))
		{
			int new_sock;
			if ((new_sock = accept(master_sock, (struct sockaddr *)&client, (socklen_t *)&c)) < 0)
			{
				perror("accept");
				exit(EXIT_FAILURE);
			}

			// inform user of socket number - used in send and receive commands
			printf("New connection , socket fd is %d , ip is : %s , port : %d \n",
				   new_sock, inet_ntoa(client.sin_addr), ntohs(client.sin_port));
			client_sock.push_back(new_sock);
		}

		// else its some IO operation on some other socket
		for (int i = 0; i < client_sock.size(); i++)
		{
			int sd = client_sock[i];

			if (FD_ISSET(sd, &readfds))
			{
				// Check if it was for closing , and also read the
				// incoming message
				int valread;
				if ((valread = read(sd, client_message, 1024)) == 0)
				{
					printf("Socket %d disconnected\n", sd);
					if (sockFD_txn.find(sd) != sockFD_txn.end())
					{
						for (auto rtbl : txnMap[sockFD_txn[sd]]->readTables)
						{
							std::string dbname = deconcat_db_table(std::string(rtbl)).first;
							std::string tablename = deconcat_db_table(std::string(rtbl)).second;
							global_tbl_readers[dbname][tablename]--;
						}
						for (auto wtbl : txnMap[sockFD_txn[sd]]->writeTables)
						{
							std::string dbname = deconcat_db_table(std::string(wtbl)).first;
							std::string tablename = deconcat_db_table(std::string(wtbl)).second;
							global_tbl_writers[dbname][tablename] = 0;
						}
						printf("Transaction %d is forcefully aborted\n", sockFD_txn[sd]);
						delete txnMap[sockFD_txn[sd]];
						txnMap.erase(sockFD_txn[sd]);
						sockFD_txn.erase(sd);
					}
					close(sd);
					client_sock.erase(client_sock.begin() + i);
				}

				// Echo the correct code
				else
				{
					client_message[valread] = '\0';
					std::pair<int, int> temp = decode_lock_request(client_message, sd);
					EncodeInt(temp.second, server_message);
					send(sd, server_message, 4, 0);
				}
			}
		}
	}

	return 0;
}