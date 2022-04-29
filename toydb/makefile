CC=g++-11
CFLAGS = -g
OBJ = ./dblayer/tbl.o ./dblayer/codec.o ./dblayer/util.o ./pflayer/pflayer.a ./amlayer/amlayer.a

all: table.o schema.o txnmgr.o main.o user.o db.o client.o queryobj.o
	$(CC) -o main $(OBJ) schema.o table.o user.o db.o main.o
	$(CC) -o txnmgr.bin $(OBJ) schema.o table.o user.o db.o txnmgr.o

client.o: api/client.cpp
	$(CC) -g -c api/client.cpp

queryobj.o: api/queryobj.cpp
	$(CC) -g -c api/queryobj.cpp

txnmgr.o: txnmgr.cpp
	$(CC) -c -g txnmgr.cpp
main.o: main.cpp
	$(CC) -c -g main.cpp
schema.o: schema.cc
	$(CC) -c -g schema.cc
table.o: table.cc
	$(CC) -c -g table.cc
user.o: user.cc
	$(CC) -c -g user.cc
db.o: db.cc
	$(CC) -c -g db.cc

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

clean:
	rm -f *.o