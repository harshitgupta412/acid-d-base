#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "codec.h"
#include "../pflayer/pf.h"
#include "../amlayer/am.h"
#include "tbl.h"
#include "util.h"

#define checkerr(err, message) {if (err < 0) {PF_PrintError(message); exit(1);}}

#define MAX_PAGE_SIZE 4000


#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0"
#define CSV_NAME "data.csv"


void
printRow(void *callbackObj, RecId rid, byte *row, int len) {
    Schema_ *schema = (Schema_ *) callbackObj;
    // +2 because the first 2 bytes store the length of the record
    byte *cursor = row+2;
    // UNIMPLEMENTED;
    // len is the record length, we decrement it to keep a track of remaining space 
    for(int i = 0; i < schema -> numColumns; i++){
        if((schema -> columns[i]) .type == INT){
            if(i != schema -> numColumns-1){
                printf("%d,", DecodeInt(cursor));
            }
            else{
                printf("%d\n", DecodeInt(cursor));
            }
            cursor += 4;
            len -= 4;
        }
        else if((schema -> columns[i]).type == VARCHAR){
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

Schema_ *
loadCSV() {
    // Open csv file, parse schema
    FILE *fp = fopen(CSV_NAME, "r");
    if (!fp) {
	perror("data.csv could not be opened");
        exit(EXIT_FAILURE);
    }

    char buf[MAX_LINE_LEN];
    char *line = fgets(buf, MAX_LINE_LEN, fp);
    if (line == NULL) {
	fprintf(stderr, "Unable to read data.csv\n");
	exit(EXIT_FAILURE);
    }

    // Open main db file
    Schema_ *sch = parseSchema(line);
    for (int i = 0; i < sch -> numColumns; i++) {
        if (sch -> columns[i].type == VARCHAR)
            printf("varchar,");
        else if (sch -> columns[i].type == INT)
            printf("int,");
        else if (sch -> columns[i].type == LONG)
            printf("long,");
        else if (sch -> columns[i].type == FLOAT)
            printf("float,");
        else
            printf("error");
    }
    printf("\n");
    
    Table_ *tbl;
    checkerr(Table_Open(DB_NAME, sch, true, &tbl), "Loadcsv : table open");
    AM_DestroyIndex(DB_NAME, 0);
    char attrType[] = {'i'};
    int attrLen[] = {4};
    assert(AM_CreateIndex(DB_NAME, 0, attrType, attrLen, 1) == AME_OK);
    int index_fileDesc = PF_OpenFile(INDEX_NAME);

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
    printf("Line: %s\n", line);
	int n = split(line, ",", tokens);
	assert (n == sch->numColumns);
	int len = encode(sch, tokens, record, sizeof(record));
    char* fields[MAX_TOKENS];
	RecId rid = 1;

    Table_Insert(tbl, record, len, &rid);
	printf("Got token: %d %s\n", rid, tokens[0]);

    decode(sch, fields, record, len);

    for(int i=0;i < sch->numColumns; i++) 
        printf("Check: %d %s ", i, fields[i]);
	// Indexing on the population column 
	int population = atoi(tokens[2]);

	// Use the population field as the field to index on
    byte population_bytes[4];
    EncodeInt(population, population_bytes);
	checkerr(AM_InsertEntry(index_fileDesc, attrType, attrLen, 1, population_bytes, rid), "Loadcsv : index insert");
    }
    // Table_Close(tbl);
    // Table_Open(DB_NAME, sch, false, &tbl);
    Table_Scan(tbl, (void *)sch, printRow);
    fclose(fp);
    Table_Close(tbl);
    checkerr(PF_CloseFile(index_fileDesc), "Loadcsv : close file");
    return sch;
}

int
main() {
    loadCSV();
}
