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

void print_all_globals()
{
	std::cout << "Global Db" << std::endl;
	for (auto str : global_db)
		std::cout << str << std::endl;
	std::cout << "\n----------------------------------------------------------------" << std::endl;

	std::cout << "Global Tbl" << std::endl;
	for (auto m : global_tbl)
	{
		std::cout << "DB Name: " << m.first << std::endl;
		for (auto str : m.second)
			std::cout << str << std::endl;
	}
	std::cout << "\n----------------------------------------------------------------" << std::endl;

	std::cout << "Global Usr" << std::endl;
	for (auto str : global_usr)
		std::cout << str << std::endl;
	std::cout << "\n----------------------------------------------------------------" << std::endl;

	std::cout << "Global Usr Db Perm" << std::endl;
	for (auto m : global_usr_db_perm)
	{
		std::cout << "User Name: " << m.first << std::endl;
		for (auto m1 : m.second)
			std::cout << m1.first << " " << m1.second << std::endl;
	}
	std::cout << "\n----------------------------------------------------------------" << std::endl;

	std::cout << "Global Usr Tbl Perm" << std::endl;
	for (auto m : global_usr_tbl_perm)
	{
		std::cout << "User Name: " << m.first << std::endl;
		for (auto m1 : m.second)
		{
			std::cout << "Db Name: " << m1.first << std::endl;
			for (auto m2 : m1.second)
				std::cout << m2.first << " " << m2.second << std::endl;
		}
	}
	std::cout << "\n----------------------------------------------------------------" << std::endl;
}

void initialize_globals()
{
	createUserDb();
	createPrivilegeTable();
	createPrivilegeDb();
	createDbList();
	createDbTableList();

	User u("SUPERUSER", "SUPERUSER_PASSWORD");
	Database db(&u);
	bool ret = db.connect("DB");
	if (ret)
	{
		printf("Connected to DB\n");
	}
	else
	{
		printf("Failed to connect to DB\n");
		exit(1);
	}

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
	// std::cout << "DB CROSS TABLE LOADED" << std::endl;
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string db_name = std::string((char *)(dbs[i].second[0]));
		std::string tbl_name = std::string((char *)(dbs[i].second[1]));
		global_tbl[db_name].insert(tbl_name);
	}
	tbl->close();

	// users
	ret = db.connect("DB_USER_DB");
	tbl = db.load("DB_USER_TABLE");
	dbs = tbl->get_records();
	for (int i = 0; i < dbs.size(); i++)
	{
		std::string user_name = std::string((char *)(dbs[i].second[0]));
		global_usr.insert(user_name);
	}

	// base perm
	for (auto usr : global_usr)
	{
		int perm = 0;
		if (usr == "SUPERUSER")
			perm = 2;
		for (auto db : global_db)
		{
			global_usr_db_perm[usr][db] = perm;
			for (auto tbl : global_tbl[db])
				global_usr_tbl_perm[usr][db][tbl] = perm;
		}
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
	std::unordered_map<std::string, int> access;
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
		DecodeCString2(buffer + 4, &username, 50);
		printf("Username: %s\n", username);
		txnID++;
		txnMap[txnID] = new Transaction();
		txnMap[txnID]->username = std::string(username);
		sockFD_txn[sockfd] = txnID;
		free(username);
		return std::make_pair(0, txnID);

	// read request
	case 1:
		printf("Read Lock Request\n");
		DecodeCString2(buffer + 4, &db_table_name, 50);
		if (txnMap[sockFD_txn[sockfd]]->access.find(std::string(db_table_name)) != txnMap[sockFD_txn[sockfd]]->access.end())
		{
			printf("Already granted\n");
			return std::make_pair(1, 1);
		}
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
		printf("Granted\n");
		global_tbl_readers[dbname][tablename]++;
		txnMap[sockFD_txn[sockfd]]->access[std::string(db_table_name)] = 1;
		free(db_table_name);
		return std::make_pair(1, 1);

	// write request
	case 2:
		printf("Write Lock Request\n");
		DecodeCString2(buffer + 4, &db_table_name, 50);
		if (txnMap[sockFD_txn[sockfd]]->access.find(std::string(db_table_name)) != txnMap[sockFD_txn[sockfd]]->access.end())
		{
			if (txnMap[sockFD_txn[sockfd]]->access[std::string(db_table_name)] == 1)
			{
				printf("Already granted a read lock. Attempting upgrade.\n");
				if (global_usr_tbl_perm[txnMap[sockFD_txn[sockfd]]->username][dbname][tablename] < 2)
				{
					printf("User %s does not have write permission on table %s.%s\n", txnMap[sockFD_txn[sockfd]]->username.c_str(), dbname.c_str(), tablename.c_str());
					return std::make_pair(2, 0);
				}
				if (global_tbl_readers[dbname][tablename] != 1)
				{
					printf("Table %s.%s is currently being read from by another transaction\n", dbname.c_str(), tablename.c_str());
					return std::make_pair(2, 0);
				}
				global_tbl_readers[dbname][tablename]--;
				global_tbl_writers[dbname][tablename] = 1;
				return std::make_pair(2, 1);
			}
			else
			{
				printf("Already granted\n");
				return std::make_pair(2, 1);
			}
		}
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
		printf("Granted\n");
		global_tbl_writers[dbname][tablename] = 1;
		txnMap[sockFD_txn[sockfd]]->access[std::string(db_table_name)] = 2;
		free(db_table_name);
		return std::make_pair(1, 1);

	// end txn request
	case 3:
		printf("End Transaction Request\n");
		if (sockFD_txn.find(sockfd) == sockFD_txn.end())
		{
			printf("No transaction associated with socket %d\n", sockfd);
			return std::make_pair(3, 0);
		}
		for (auto x : txnMap[sockFD_txn[sockfd]]->access)
		{
			dbname = deconcat_db_table(std::string(x.first)).first;
			tablename = deconcat_db_table(std::string(x.first)).second;
			if (x.second == 1)
			{
				global_tbl_readers[dbname][tablename]--;
			}
			else if (x.second == 2)
			{
				global_tbl_writers[dbname][tablename] = 0;
			}
			else
			{
				fprintf(stderr, "Invalid access value observed in transaction %d\n", sockFD_txn[sockfd]);
				assert(0);
			}
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
	for (auto x : txn.access)
	{
		printf("%s %d\n", x.first.c_str(), x.second);
	}
}

/* END Transaction */

int main(int argc, char *argv[])
{
	initialize_globals();
	print_all_globals();

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

	while (1)
	{
		fd_set readfds;
		int max_sd;
		// clear the socket set
		FD_ZERO(&readfds);
		// add master socket to set
		FD_SET(master_sock, &readfds);
		max_sd = master_sock;

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
			break;
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
				if ((valread = read(sd, client_message, 1024)) <= 0)
				{
					if (sockFD_txn.find(sd) != sockFD_txn.end())
					{
						for (auto x : txnMap[sockFD_txn[sd]]->access)
						{
							std::string dbname = deconcat_db_table(std::string(x.first)).first;
							std::string tablename = deconcat_db_table(std::string(x.first)).second;
							if (x.second == 1)
							{
								global_tbl_readers[dbname][tablename]--;
							}
							else if (x.second == 2)
							{
								global_tbl_writers[dbname][tablename] = 0;
							}
							else
							{
								fprintf(stderr, "Invalid access value observed in transaction %d\n", sockFD_txn[sd]);
								assert(0);
							}
						}
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
					if (temp.first != 3)
					{
						EncodeInt(temp.second, server_message);
						send(sd, server_message, 4, 0);
					}
				}
			}
		}
	}

	close(master_sock);

	return 0;
}