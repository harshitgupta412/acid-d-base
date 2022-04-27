#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>
#include <stdlib.h>
#include "codec.h"
#define VARCHAR 1
#define INT     2
#define LONG    3
#define FLOAT   4
typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc *columns; // array of column descriptors
} Schema_;

typedef struct {
    Schema_ *schema;
    int fileDesc;
    int lastPageNo;
    char* name;
} Table_ ;

typedef int RecId;

int
Table_Open(char *fname, Schema_ *schema, bool overwrite, Table_ **table);

int
Table_Insert(Table_ *t, byte *record, int len, RecId *rid);

int
Table_Get(Table_ *t, RecId rid, byte *record, int maxlen);

void
Table_Close(Table_ *);

typedef void (*ReadFunc)(void *callbackObj, RecId rid, byte *row, int len);

void
Table_Scan(Table_ *tbl, void *callbackObj, ReadFunc callbackfn);


RecId
Table_Search(Table_ *tbl, int pk_index[], byte* pk_value[], int numAttr) ;

void Table_Delete(Table_ *tbl, RecId rid);
/*
Takes a schema, and an array of strings (fields), and uses the functionality
in codec.c to convert strings into compact binary representations
 */
int
encode(Schema_ *sch, char **fields, byte *record, int spaceLeft);

int
decode(Schema_ *sch, char *fields[], byte *record, int len);

char* getNthfield(char* record, int n, Schema_* sch);


#endif

