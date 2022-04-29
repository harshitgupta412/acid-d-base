CC=g++-11
CFLAGS = -g
OBJ = ./dblayer/tbl.o ./dblayer/codec.o ./dblayer/util.o ./pflayer/pflayer.a ./amlayer/amlayer.a

all: table.o schema.o txnmgr.o main.o user.o db.o
	$(CC) -o main $(OBJ) schema.o table.o user.o db.o main.o
	$(CC) -o txnmgr.bin $(OBJ) schema.o table.o user.o db.o txnmgr.o

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

clean:
	rm -f *.o