CC=g++-11
CFLAGS = -g
OBJ = ./dblayer/tbl.o ./dblayer/codec.o ./dblayer/util.o ./pflayer/pflayer.a ./amlayer/amlayer.a SHA256.o

all: table.o schema.o txnmgr.o main.o user.o db.o client.o queryobj.o txnmgr.o binaryops.o api.o api2.o api3.o
	$(CC) -o main.bin $(OBJ) schema.o table.o user.o db.o main.o
	$(CC) -o txnmgr.bin $(OBJ) schema.o table.o user.o db.o txnmgr.o
	$(CC) -o binaryops.bin $(OBJ) schema.o table.o user.o db.o binaryops.o
	$(CC) -o api.bin $(OBJ) schema.o table.o user.o db.o client.o queryobj.o api.o
	$(CC) -o api2.bin $(OBJ) schema.o table.o user.o db.o client.o queryobj.o api2.o
	$(CC) -o api3.bin $(OBJ) schema.o table.o user.o db.o client.o queryobj.o api3.o
	$(CC) -o tests/query_union.bin  tests/query_union.cc $(OBJ) schema.o table.o user.o db.o
	$(CC) -o tests/query_project.bin  tests/query_project.cc $(OBJ) schema.o table.o user.o db.o
	$(CC) -o tests/query_intersect.bin  tests/query_intersect.cc $(OBJ) schema.o table.o user.o db.o

api3.o: api/api3.cpp client.o queryobj.o db.o
	$(CC) -g -c api/api3.cpp

api2.o: api/api2.cpp client.o queryobj.o db.o
	$(CC) -g -c api/api2.cpp

api.o: api/api.cpp client.o queryobj.o db.o
	$(CC) -g -c api/api.cpp

client.o: api/client.cpp user.o
	$(CC) -g -c api/client.cpp

queryobj.o: api/queryobj.cpp
	$(CC) -g -c api/queryobj.cpp

binaryops.o: binaryops.cpp
	$(CC) -g -c binaryops.cpp

txnmgr.o: txnmgr.cpp
	$(CC) -c -g txnmgr.cpp
main.o: main.cpp
	$(CC) -c -g main.cpp
schema.o: schema.cc
	$(CC) -c -g schema.cc
table.o: table.cc
	$(CC) -c -g table.cc
user.o: user.cc SHA256.o
	$(CC) -c -g user.cc
db.o: db.cc
	$(CC) -c -g db.cc

SHA256.o:
	$(CC) -c -g SHA256.cpp

load_data: schema.o user.o db.o table.o 
	$(CC) -o tests/load tests/load_data.cc $(OBJ) schema.o table.o user.o db.o  

test_query: schema.o user.o db.o table.o 
	$(CC) -o tests/query tests/query.cc $(OBJ) schema.o table.o user.o db.o  

join_query: schema.o user.o db.o table.o 
	$(CC) -o tests/join tests/join.cc $(OBJ) schema.o table.o user.o db.o  

test_query_index: schema.o user.o db.o table.o 
	$(CC) -o tests/query_index tests/queryindex.cc $(OBJ) schema.o table.o user.o db.o

test_query_index_int: schema.o user.o db.o table.o 
	$(CC) -o tests/query_index_int tests/query_index_int.cc $(OBJ) schema.o table.o user.o db.o  

add_delete: schema.o user.o db.o table.o 
	$(CC) -o tests/add_delete tests/normal.cc $(OBJ) schema.o table.o user.o db.o  

add_delete_table: schema.o user.o db.o table.o 
	$(CC) -o tests/add_delete_table tests/add_delete_table.cc $(OBJ) schema.o table.o user.o db.o  

indexes: schema.o user.o db.o table.o 
	$(CC) -o tests/indexes tests/indexes.cc $(OBJ) schema.o table.o user.o db.o     

user_test: schema.o user.o db.o table.o 
	$(CC) -o tests/user_permission tests/user.cc $(OBJ) schema.o table.o user.o db.o          

clean:
	rm -f *.o