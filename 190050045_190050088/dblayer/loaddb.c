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
    Table_ *tbl;
    checkerr(Table_Open(DB_NAME, sch, true, &tbl), "Loadcsv : table open");
    AM_DestroyIndex(DB_NAME, 0);
    assert(AM_CreateIndex(DB_NAME, 0, 'i', 4) == AME_OK);
    int index_fileDesc = PF_OpenFile(INDEX_NAME);

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
	int n = split(line, ",", tokens);
	assert (n == sch->numColumns);
	int len = encode(sch, tokens, record, sizeof(record));
	RecId rid;

    Table_Insert(tbl, record, len, &rid);
	printf("%d %s\n", rid, tokens[0]);

	// Indexing on the population column 
	int population = atoi(tokens[2]);

	// Use the population field as the field to index on
    byte population_bytes[4];
    EncodeInt(population, population_bytes);
	AM_InsertEntry(index_fileDesc, 'i', 4, population_bytes, rid);
    }

    fclose(fp);
    Table_Close(tbl);
    checkerr(PF_CloseFile(index_fileDesc), "Loadcsv : close file");
    return sch;
}

int
main() {
    loadCSV();
}
