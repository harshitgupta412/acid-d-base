CC=g++-11
CFLAGS = -g
OBJ = ./dblayer/tbl.o ./dblayer/codec.o ./dblayer/util.o ./pflayer/pflayer.a ./amlayer/amlayer.a

all: table.o schema.o main.o
	$(CC) -o main main.o schema.o table.o $(OBJ)

main.o:
	$(CC) -c -g main.cpp
schema.o:
	$(CC) -c -g schema.cc
table.o:
	$(CC) -c -g table.cc

clean:
	rm *.o