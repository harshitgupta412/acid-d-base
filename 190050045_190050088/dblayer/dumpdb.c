#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "codec.h"
#include "tbl.h"
#include "util.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#define checkerr(err) {if (err < 0) {PF_PrintError(); exit(1);}}

#define MAX_PAGE_SIZE 4000

void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema_ *schema = (Schema_ *) callbackObj;
    // +2 because the first 2 bytes store the length of the record
    byte *cursor = row+2;
    // UNIMPLEMENTED;
    // len is the record length, we decrement it to keep a track of remaining space 
    for(int i = 0; i < schema -> numColumns; i++){
        if((schema -> columns[i]) -> type == INT){
            if(i != schema -> numColumns-1){
                printf("%d,", DecodeInt(cursor));
            }
            else{
                printf("%d\n", DecodeInt(cursor));
            }
            cursor += 4;
            len -= 4;
        }
        else if((schema -> columns[i]) -> type == VARCHAR){
            short string_len = DecodeShort(cursor);
            char x[len];
            int str_len = DecodeCString(cursor, x, len)+2;
            cursor += str_len;
            len -= str_len;
            if(i != schema -> numColumns-1){
                printf("%s,", x);
            }
            else{
                printf("%s\n", x);
            }
        }
        else{
            if(i != schema -> numColumns-1){
                printf("%lld,", DecodeLong(cursor));
            }
            else{
                printf("%lld\n", DecodeLong(cursor));
            }
            cursor+=8;
            len -= 8;
        }
    }
}

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
	 
void
index_scan(Table_ *tbl, Schema_ *schema, int indexFD, int op, int value) {
    // UNIMPLEMENTED;
    /*
    Open index ...
    while (true) {
	find next entry in index
	fetch rid from table
        printRow(...)
    }
    close index ...
    */
   byte population_bytes[4];
   EncodeInt(value, population_bytes);
   int scanDesc = AM_OpenIndexScan(indexFD, 'i', 4, op, population_bytes);
   while(true){
       int rid = AM_FindNextEntry(scanDesc);
       if(rid == AME_EOF) break;
       char record[MAX_PAGE_SIZE];
       int num_bytes = Table_Get(tbl, rid, record, MAX_PAGE_SIZE);
       printRow((void *)schema, rid, record, num_bytes);
   }
   AM_CloseIndexScan(scanDesc);
}

int
main(int argc, char **argv) {
    char *schemaTxt = "Country:varchar,Capital:varchar,Population:int";
    Schema_ *schema = parseSchema(schemaTxt);
    Table_ *tbl;

    // UNIMPLEMENTED;
    Table_Open(DB_NAME, schema, false, &tbl);
    if (argc == 2 && *(argv[1]) == 's') {
	    // UNIMPLEMENTED;
        Table_Scan(tbl, (void *)schema, printRow);
	// invoke Table_Scan with printRow, which will be invoked for each row in the table.
    } else if(argc == 2){
	// index scan by default
	int indexFD = PF_OpenFile(INDEX_NAME);
	checkerr(indexFD);

	// Ask for populations less than 100000, then more than 100000. Together they should
	// yield the complete database.
	index_scan(tbl, schema, indexFD, LESS_THAN_EQUAL, 100000);
	index_scan(tbl, schema, indexFD, GREATER_THAN, 100000);
    } else if(argc == 4){
        int op;
        if(strcmp(argv[2], "EQUAL") == 0) op = EQUAL;
        else if(strcmp(argv[2], "LESS_THAN_EQUAL") == 0) op = LESS_THAN_EQUAL;
        else if(strcmp(argv[2], "LESS_THAN") == 0) op = LESS_THAN;
        else if(strcmp(argv[2], "GREATER_THAN") == 0)op = GREATER_THAN;
        else op = GREATER_THAN_EQUAL;

        int value = atoi(argv[3]);

        int indexFD = PF_OpenFile(INDEX_NAME);
	    checkerr(indexFD);
        index_scan(tbl, schema, indexFD, op, value);
    }
    Table_Close(tbl);
}
