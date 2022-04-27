#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "table.h"
#define checkerr(err, message) {if (err < 0) {PF_PrintError(message); exit(1);}}

#define MAX_PAGE_SIZE 4000
#define MAX_TOKENS 100
#define MAX_LINE_LEN   1000

#define DB_NAME "data.db"
#define INDEX_NAME "data.db.0.idx"
#define CSV_NAME "./dblayer/data.csv"

using namespace std;

int
stricmp(char const *a, char const *b)
{
    for (;; a++, b++) {
        int d = tolower((unsigned char)*a) - tolower((unsigned char)*b);
        if (d != 0 || !*a)
            return d;
    }
}

char *trim(char *str)
{
  char *end;

  // Trim leading space
  while(isspace((unsigned char)*str)) str++;

  if(*str == 0)  // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while(end > str && isspace((unsigned char)*end)) end--;

  // Write new null terminator character
  end[1] = '\0';

  return str;
}


/**
  split takes a character buf, and uses strtok to populate
  an array of token*'s. 
*/
int 
split(char *buf, char *delim, char **tokens) {
    char *token = strtok(buf, delim);
    int n = 0;
    while(token) {
	tokens[n] = trim(token);
	token = strtok(NULL, delim);
	n++;
    }
    return n;
}

Schema_ *
parseSchema(char *buf) {
    buf = strdup(buf);
    char *tokens[MAX_TOKENS];
    int n = split(buf, ",", tokens);
    Schema_ *sch = (Schema_*)malloc(sizeof(Schema_));
    sch->columns = (ColumnDesc*)malloc(n * sizeof(ColumnDesc *));
    // strtok is terrible; it depends on global state.
    // Do one split based on ',".
    // Could use strtok_s for this use case
    char *descTokens[MAX_TOKENS];
    sch->numColumns = n;
    for (int i = 0; i < n; i++) {
	int c = split(tokens[i],":", descTokens);
	assert(c == 2);
	ColumnDesc *cd = (ColumnDesc *) malloc(sizeof(ColumnDesc));
	cd->name = strdup(descTokens[0]);
	char *type = descTokens[1];
	int itype = 0;
	if (stricmp(type, "varchar") == 0) {
	    itype = VARCHAR;
	} else if (stricmp(type, "int") == 0) {
	    itype = INT;
	} else if (stricmp(type, "long") == 0) {
	    itype = LONG;
	} else {
	    fprintf(stderr, "Unknown type %s \n", type);
	    exit(EXIT_FAILURE);
	}
	cd->type = itype;
	sch->columns[i] = *cd;
    }
    free(buf);
    return sch;
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
    Schema s(sch, vector<int>(1,0));
    Table tbl(&s,DB_NAME,"",true,vector<IndexData>());
    // checkerr(Table_Open(DB_NAME, sch, true, &tbl), "Loadcsv : table open");
    // AM_DestroyIndex(DB_NAME, 0);
    // assert(AM_CreateIndex(DB_NAME, 0, 'i', 4) == AME_OK);
    // int index_fileDesc = PF_OpenFile(INDEX_NAME);
    cout<<"Created Table"<<endl;
    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
	cout<<line<<endl;
	int n = split(line, ",", tokens);
	assert (n == sch->numColumns);
	cout<<"line"<<endl;
    tbl.addRow((void**)tokens,false);
    
    // int len = encode(sch, tokens, record, sizeof(record));
	// RecId rid;

    // Table_Insert(tbl, record, len, &rid);
	// printf("%d %s\n", rid, tokens[0]);

	// Indexing on the population column 
	// int population = atoi(tokens[2]);

	// Use the population field as the field to index on
    // byte population_bytes[4];
    // EncodeInt(population, population_bytes);
	// AM_InsertEntry(index_fileDesc, 'i', 4, population_bytes, rid);
    }
    tbl.print();
    cout<<"----------------------------------------------------------------"<<endl;
    char* tokens2[1];
    tokens2[0] = new char[100];
    tokens2[0] = "Zimbabwe"; 
    char** data= (char**)tbl.getRow((void**)tokens2);
    for(int i=0; i<sch->numColumns; i++) {
        switch(sch->columns[i].type) {
            case INT:
                printf("%d\t", DecodeInt(data[i]));
                break;
            case FLOAT:
                printf("%f\t", DecodeFloat(data[i]));
                break;
            case LONG:
                printf("%lld\t", DecodeLong(data[i]));
                break;
            case VARCHAR:
                printf("%s\t", (char*)data[i]);
                break;
            default:
                printf("%s\t", (char*)data[i]);
                break;
        }
    }
    cout<<"\n----------------------------------------------------------------"<<endl;

    cout<<tbl.deleteRow((void**)tokens2)<<' ';
    cout<<"deleted"<<endl;
    tbl.print();
    
    char name[] = "Zimbabwe";
    char capital[] = "Harare";
    char pop[] = "16529904";
    void *data2[3] = {(void*)name, (void*)capital, (void*)pop};
    tbl.addRow((void**)data2,false);

    tbl.print();

    cout<<"----------------------------------------------------------------"<<endl;
    tbl.close();
    // fclose(fp);
    // Table_Close(tbl);
    // checkerr(PF_CloseFile(index_fileDesc), "Loadcsv : close file");
    return sch;
}

int
main() {
    loadCSV();
}
