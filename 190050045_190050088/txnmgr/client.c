#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "codec.h"

int main(int argc , char *argv[])
{
	int sock;
	struct sockaddr_in server;
	char message[1000] , server_reply[2000];
	
	//Create socket
	sock = socket(AF_INET , SOCK_STREAM , 0);
	if (sock == -1)
	{
		printf("Could not create socket");
	}
	puts("Socket created");
	
	server.sin_addr.s_addr = inet_addr("127.0.0.1");
	server.sin_family = AF_INET;
	server.sin_port = htons( 8900 );

	//Connect to remote server
	if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
	{
		perror("connect failed. Error");
		return 1;
	}
	
	puts("Connected\n");
	
		char* buf = message;
		buf += EncodeCString("user1", buf, MAX_STR_LEN);
		buf += EncodeInt(2, buf);
		buf += EncodeCString("db1", buf, MAX_STR_LEN);
		buf += EncodeCString("tbl1", buf, MAX_STR_LEN);
		buf += EncodeInt(0, buf);
		buf += EncodeCString("db2", buf, MAX_STR_LEN);
		buf += EncodeCString("tbl2", buf, MAX_STR_LEN);
		buf += EncodeInt(1, buf);

		//Send some data
		if( send(sock , message , buf - message , 0) < 0)
		{
			puts("Send failed");
			return 1;
		}
		
		//Receive a reply from the server
        int read_size;
		if( (read_size = recv(sock , server_reply , 2000 , 0)) <= 0)
		{
            if (read_size == 0)
            {
                puts("Server disconnected");
            }
            else
            {
                perror("recv failed");
            }
		}
		server_reply[read_size] = '\0';
		puts("Server reply :");
		puts(server_reply);
	
	close(sock);
	return 0;
}