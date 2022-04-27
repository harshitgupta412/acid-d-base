CC=g++-11
CFLAGS = -g
OBJ = ./dblayer/tbl.o ./dblayer/codec.o ./dblayer/util.o ./pflayer/pflayer.a ./amlayer/amlayer.a

all: table.o schema.o main.o user.o db.o
	$(CC) -o main main.o schema.o table.o user.o $(OBJ)

main.o:
	$(CC) -c -g main.cpp
schema.o:
	$(CC) -c -g schema.cc
table.o:
	$(CC) -c -g table.cc
user.o:
	$(CC) -c -g user.cc
db.o:
	$(CC) -c -g db.cc

clean:
	rm -f *.o