#ifndef _TBL_H_
#define _TBL_H_
#include <stdbool.h>
#include <codec.h>
#define VARCHAR 1
#define INT     2
#define LONG    3

typedef char byte;

typedef struct {
    char *name;
    int  type;  // one of VARCHAR, INT, LONG
} ColumnDesc;

typedef struct {
    int numColumns;
    ColumnDesc **columns; // array of column descriptors
} Schema_;

typedef struct {
    Schema_ *schema;
    int fileDesc;
    byte* pageBuf;
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
encode(Schema_ *sch, char **fields, byte *record, int spaceLeft) {
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : EncodeCString
    //        INT : EncodeInt
    //        LONG: EncodeLong
    // return the total number of bytes encoded into record
    int bytes_encoded = 0;
    for ( int i = 0; i < sch->numColumns; ++i )
    {
        switch ( sch->columns[i]->type )
        {
            case VARCHAR:
            {
                int len = EncodeCString(fields[i], record, spaceLeft);
                spaceLeft -= len;
                record += len;
                bytes_encoded += len;
                break;
            }
            case INT:
            {
                assert(spaceLeft >= 4);
                EncodeInt(atoi(fields[i]), record);
                spaceLeft -= 4;
                record += 4;
                bytes_encoded += 4;
                break;
            }
            case LONG:
            {
                assert(spaceLeft >= 8);
                EncodeLong(atol(fields[i]), record);
                spaceLeft -= 8;
                record += 8;
                bytes_encoded += 8;
                break;
            }
            default:
            {
                printf("Unknown type %d\n", sch->columns[i]->type);
                exit(1);
            }
        }
    }
    return bytes_encoded;
}

int
decode(Schema_ *sch, char *fields[], byte *record, int len) {
    // for each field
    //    switch corresponding schema type is
    //        VARCHAR : DecodeCString
    //        INT : DecodeInt
    //        LONG: DecodeLong
    // return the total number of bytes decoded into record
    int bytes_decoded = 0;
    byte *cursor = record + 2;

    for ( int i = 0; i < sch->numColumns; ++i )
    {
        switch ( sch->columns[i]->type )
        {
            case VARCHAR:
            {
                short string_len = DecodeShort(cursor);
                char x[len];
                int strlen = DecodeCString(cursor, x, len)+2;
                cursor += strlen;
                bytes_decoded += strlen;
                len -= strlen;
                fields[i] = (char *)malloc(string_len+1);
                strncpy(fields[i], x, string_len);
                fields[i][string_len] = '\0';
                break;
            }
            case INT:
            {
                fields[i] = (char*)malloc(sizeof(int));
                fields[i] = DecodeInt(cursor);
                cursor += 4;
                bytes_decoded += 4;
                len -= 4;
                break;
            }
            case LONG:
            {
                fields[i] = (char*)malloc(sizeof(long));
                fields[i] = DecodeLong(cursor);
                cursor += 8;
                bytes_decoded += 8;
                len -= 8;
                break;
            }
            default:
            {
                printf("Unknown type %d\n", sch->columns[i]->type);
                exit(1);
            }
        }
    }
    return bytes_decoded;
}
#endif
