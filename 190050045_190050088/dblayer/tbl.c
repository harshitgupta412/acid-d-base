#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tbl.h"
#include "codec.h"
#include "../pflayer/pf.h"

#define SLOT_OFFSET 2
#define HEADER_SIZE_OFFSET 2
#define SIZE_OFFSET 2

#define END_OF_PAGE PF_PAGE_SIZE

#define checkerr(err, message) {if (err < 0) {PF_PrintError(message); exit(EXIT_FAILURE);}}

// set the number of slots in newly initialised page to be 0
void initializePage(byte* pageBuf)
{
    EncodeShort(0, pageBuf);
}

// get number of slots in the page buffer
int  getNumSlots(byte *pageBuf)
{
    return DecodeShort(pageBuf);
}

// get offset of the Nth slot, where N is 0 indexed
int  getNthSlotOffset(int slot, char* pageBuf)
{
    assert(slot < getNumSlots(pageBuf));
    return DecodeShort(pageBuf + HEADER_SIZE_OFFSET + slot * SLOT_OFFSET);
}

// get length of the `slot` record
int  getLen(int slot, byte *pageBuf)
{
    int offset = getNthSlotOffset(slot, pageBuf);
    if(offset == -1) return -1;
    return DecodeShort(pageBuf + offset);
}

// insert a new record into the database and appropriately update the headers
int insertRecord(byte* pageBuf, byte* record, int len)
{
    int nslots = getNumSlots(pageBuf);
    int offset;
    if(nslots == 0) offset = END_OF_PAGE;
    else {
        int rid = nslots-1;
        while(rid >= 0 && getNthSlotOffset(rid, pageBuf) == -1) rid--;
        if(rid == -1) offset = END_OF_PAGE;
        else offset = getNthSlotOffset(rid, pageBuf);
    }
    int freeSpace = offset - (HEADER_SIZE_OFFSET + nslots * SLOT_OFFSET);
    if (freeSpace < len + SIZE_OFFSET + SLOT_OFFSET)
        return -1;
    offset -= len + SIZE_OFFSET;
    EncodeShort(len, pageBuf + offset);
    memcpy(pageBuf + offset + SIZE_OFFSET, record, len);
    EncodeShort(offset, pageBuf + HEADER_SIZE_OFFSET + nslots * SLOT_OFFSET);
    EncodeShort(nslots + 1, pageBuf);
    return 0;
}

/**
   Opens a paged file, creating one if it doesn't exist, and optionally
   overwriting it.
   Returns 0 on success and a negative error code otherwise.
   If successful, it returns an initialized Table_*.
 */
int
Table_Open(char *dbname, Schema_ *schema, bool overwrite, Table_ **ptable)
{
    // Initialize PF, create PF file,
    // allocate Table_ structure  and initialize and return via ptable
    // The Table_ structure only stores the schema. The current functionality
    // does not really need the schema, because we are only concentrating
    // on record storage. 
    PF_Init();

    int fileDesc;
    if (!overwrite)
    {
        fileDesc = PF_OpenFile(dbname);
        if (fileDesc < 0)
        {
            PF_CreateFile(dbname);
            fileDesc = PF_OpenFile(dbname);
        }
    }
    else
    {
        PF_DestroyFile(dbname);
        PF_CreateFile(dbname);
        fileDesc = PF_OpenFile(dbname);
    }
    checkerr(fileDesc, "Table_ open : file open");
    
    Table_ *table = (Table_ *) malloc(sizeof(Table_));
    table->schema = schema;
    table->fileDesc = fileDesc;

    int pageNo, err;
    char *pageBuf;
    if ( (err = PF_GetFirstPage(fileDesc, &pageNo, &pageBuf)) == PFE_EOF)
    {
        // No pages in file, so create one
        checkerr(PF_AllocPage(fileDesc, &pageNo, &pageBuf), "Table_ open : allocate page");
        initializePage(pageBuf);
    }
    else if ( err >= 0 )
    {   
        // loop so that we can reach the last page number
        do
        {
            checkerr(PF_UnfixPage(fileDesc, pageNo, 0), "Table_ Open : unfix page");
        } while( (err = PF_GetNextPage(fileDesc, &pageNo, &pageBuf)) >= 0 );
        assert(err == PFE_EOF);
    }
    else
        checkerr(err, "Table_ open : get first page");
    table->lastPageNo = pageNo;
    table->pageBuf = pageBuf;
    table->name = dbname;
    *ptable = table;
    return 0;
}

void
Table_Close(Table_ *tbl) {    
    // Unfix any dirty pages, close file.
    int err = PF_UnfixPage(tbl->fileDesc, tbl->lastPageNo, 1);
    // if the page is already unfixed, don't unfix it again
    if(err != PFE_OK && err != PFE_PAGEUNFIXED)
        checkerr(err, "Table_ close : unfix page");
    checkerr(PF_CloseFile(tbl->fileDesc), "Table_ close : close file");
}


int
Table_Insert(Table_ *tbl, byte *record, int len, RecId *rid) {
    // Allocate a fresh page if len is not enough for remaining space
    // Get the next free slot on page, and copy record in the free
    // space
    // Update slot and free space index information on top of page.
    if (insertRecord(tbl->pageBuf, record, len) < 0)
    {
        int pageNo;
        char *pageBuf;
        checkerr(PF_AllocPage(tbl->fileDesc, &pageNo, &pageBuf), "Table_ insert : allocate page");
        initializePage(pageBuf);
        checkerr(PF_UnfixPage(tbl->fileDesc, tbl->lastPageNo, 1), "Table_ insert : unfix page");
        tbl->lastPageNo = pageNo;
        tbl->pageBuf = pageBuf;
        insertRecord(tbl->pageBuf, record, len);
    }
    *rid = (tbl->lastPageNo << 16) | (getNumSlots(tbl->pageBuf) - 1);
    return 0;
}

/*
  Given an rid, fill in the record (but at most maxlen bytes).
  Returns the number of bytes copied.
 */
int
Table_Get(Table_ *tbl, RecId rid, byte *record, int maxlen) {
    int slot = rid & 0xFFFF;
    int pageNum = rid >> 16;

    // PF_GetThisPage(pageNum)
    // In the page get the slot offset of the record, and
    // memcpy bytes into the record supplied.
    // Unfix the page
    char* pageBuf;
    int err;
    if ( (err = PF_GetThisPage(tbl->fileDesc, pageNum, &pageBuf)) == PFE_PAGEFIXED );
    else checkerr(err, "Table_ get : get page");
    int len = getLen(slot, pageBuf);
    if(len == -1) return -1;
    // the first 2 bytes in record indicate the length of the record
    if (len + 2 > maxlen) len = maxlen - 2;
    memcpy(record, pageBuf + getNthSlotOffset(slot, pageBuf), len + SIZE_OFFSET);
    if (err != PFE_PAGEFIXED) PF_UnfixPage(tbl->fileDesc, pageNum, 0);
    return len; // return size of record
}

void
Table_Scan(Table_ *tbl, void *callbackObj, ReadFunc callbackfn) {
    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          callbackfn(callbackObj, rid, record, recordLen)
    int pageNo, err;
    char *pageBuf;
    if ( (err = PF_GetFirstPage(tbl->fileDesc, &pageNo, &pageBuf)) == PFE_EOF)
        return;
    else if ( err >= 0 || err == PFE_PAGEFIXED )
    {
        do
        {
            int nslots = getNumSlots(pageBuf);
            for (int i = 0; i < nslots; i++)
            {
                int len = getLen(i, pageBuf);
                if(len == -1) continue;
                callbackfn(callbackObj, (pageNo << 16) | i, pageBuf + getNthSlotOffset(i, pageBuf), len);
            }
            if (err != PFE_PAGEFIXED) checkerr(PF_UnfixPage(tbl->fileDesc, pageNo, 0), "Table_ scan : unfix page");
            err = PF_GetNextPage(tbl->fileDesc, &pageNo, &pageBuf);
        } while( err >= 0 || err == PFE_PAGEFIXED );
        assert(err == PFE_EOF);
    }
    else
        checkerr(err, "Table_ scan : get first page");
}


RecId
Table_Search(Table_ *tbl, int pk_index[], char* pk_value[], int numAttr) {
    // For each page obtained using PF_GetFirstPage and PF_GetNextPage
    //    for each record in that page,
    //          check if the record matches the search criteria
    
    int pageNo, err;
    char *pageBuf;
    if ( (err = PF_GetFirstPage(tbl->fileDesc, &pageNo, &pageBuf)) == PFE_EOF)
        return;
    else if ( err >= 0 || err == PFE_PAGEFIXED )
    {
        do
        {
            int nslots = getNumSlots(pageBuf);
            for (int i = 0; i < nslots; i++)
            {
                int len = getLen(i, pageBuf);
                if(len == -1) continue;
                char** fields = malloc(tbl->schema->numColumns * sizeof(char*));
                decode(tbl->schema, fields, pageBuf + getNthSlotOffset(i, pageBuf), len);
                for(int j=0; j<numAttr; j++) {
                    if(strcmp(fields[pk_index[j]], pk_value[j]) != 0) {
                        free(fields);
                        break;
                    }
                    if(j == numAttr-1) {
                        free(fields);
                        return (pageNo << 16) | i;
                    }
                }
            }
            if (err != PFE_PAGEFIXED) checkerr(PF_UnfixPage(tbl->fileDesc, pageNo, 0), "Table_ scan : unfix page");
            err = PF_GetNextPage(tbl->fileDesc, &pageNo, &pageBuf);
        } while( err >= 0 || err == PFE_PAGEFIXED );
        assert(err == PFE_EOF);
    }
    else
        checkerr(err, "Table_ scan : get first page");
}


void Table_Delete(Table_ *tbl, RecId rid) {
    int pageNo = rid >> 16;
    int slot = rid & 0xFFFF;
    char *pageBuf;
    int err;

    if ( (err = PF_GetThisPage(tbl->fileDesc, pageNo, &pageBuf)) == PFE_PAGEFIXED );
    else checkerr(err, "Table_ delete : get page");

    // Delete the record from the page
    // Update slot and free space index information on top of page.
    EncodeShort(-1, pageBuf + HEADER_SIZE_OFFSET + slot * SLOT_OFFSET);
}

