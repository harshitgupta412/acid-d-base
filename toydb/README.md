#CS631 Assignment.  Jan 25, 2019.

You are given a toydb with some missing parts that you have to fill in.
This assignment is conceptually split into three tasks. 

#1. Building a record layer

First, you need to fill in the missing code in nthe dblayer directory. It is a record or tuple layer on top of a physical layer library (pflayer, which is provided to you). The physical layer library presents a paged file abstraction, where a file is logically split into pages. 

You have to structure each page as a slotted-page structure. That is, the header at the top of the page must contain the following information: an array of pointers (offset within the page) to each record, the number of such records, and the pointer to the free space. The actual record data is stored bottom up from the  page. Tuples are addressed by a 4 byte 'rid' (record id), where the first 2 bytes identify a page, and the other two are an offset in the slot header.
Note that this layer treats the record as a blob of bytes, and does not know about columns or fields. 

For this part of the assignment, search for "UNMPLEMENTED" in tbl.c and tbl.h, and put in the relevant code.

#2. Testing: loading CSV data into db, and creating an index.

We will load up a table using the API above, from data contained in a CSV file. You will be supplied code in loaddb.c to do all the relevant parts; you just have to fill in the "UNIMPLEMENTED" parts.
The first line of the CSV file contains schema information; for example:
      country:varchar,  population:int, capital:varchar

The data type can be one of "varchar", "int", "long"; the maximum sizes of each field is assumed to be less than the page size, and further all the fields in a row together fit in a page.

The rough steps are as follows:
     for each row in the csv file,
          split it up into fields
	  encode each field (according to type) into one record buffer
	  rid = Table_Insert(record)
	  AM_InsertEntry(index field, rid)

The idea is to use the table API you just built, which returns a record id, then supply that record id to a BTree indexer. That code (with the prefix AM_, for access method) is made available to you. You simply have to read the docs.

#3. Testing: Retrieving the data.

Fill in code in dumpdb.c.

dumpdb has two ways to retrieve data (depending on a command-line argument).

"dumpdb s" does a sequential scan, implemennted using Table_Scan
"dumpdb i" does an index scan. Use the AM_ methods to do a scan of the index, and for each record id, invoke
Table_Get to fetch the record. 

In both cases, you have to decode the record to print it back in the same format as the csv file, so that we can compare the original CSV file with the version reconstructed from the database. There should be no difference.


# Miscellaneous details:

1. Familiarize yourself with the am.ps and pf.ps docs on the parts that are already built. You don't need to
    understand the internals though.

2. Invoke make in each of the pflayer and amlayer directories before building the dblayer.
