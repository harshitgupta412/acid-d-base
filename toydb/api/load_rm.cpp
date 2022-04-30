#include <bits/stdc++.h>
#include <utility>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>
#include "../schema.h"
#include "../table.h"
#include "../db.h"
#include "../user.h"
#include "client.h"

#define checkerr(err, message) {if (err < 0) {PF_PrintError(message); exit(1);}}

#define MAX_PAGE_SIZE 4000
#define MAX_TOKENS 100
#define MAX_LINE_LEN   1000

#define DB_NAME "SCUSTOM_SECOND_DB"
#define INDEX_NAME "FIRST_DB.0.idx"
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
    sch->columns = (ColumnDesc*)malloc(n * sizeof(ColumnDesc));
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

int main(){

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

    User u("SUPERUSER", "SUPERUSER_PASSWORD");
    Client c(&u);

    c.connect2mngr();
    c.initTxn(u);

    char *tokens[MAX_TOKENS];
    char record[MAX_PAGE_SIZE];

    cout<<"Deleting rows"<<endl;
    while ((line = fgets(buf, MAX_LINE_LEN, fp)) != NULL) {
        int n = split(line, ",", tokens);
        if (!c.del("SCUSTOM_SECOND_DB.main2",(void**)tokens)) {
            c.rollback();
            c.endTxn();
            c.disconnect();
            printf("Unable to delete row : \n");
            for (int i = 0; i < n; i++) {
                printf("%s\t", tokens[i]);
            }
            printf("\n");
            exit(1);
        }
        printf("successfully deleted : \n");
        for (int i = 0; i < n; i++) {
            printf("%s\t", tokens[i]);
        }
        printf("\n");
    }
    cout<<"----------------------------------------------------------------"<<endl;
    cout<<"Printing"<<endl;
    
    QueryObj q("SCUSTOM_SECOND_DB.main2");
    void ***result;
    int len;
    if (!c.evalQuery(q,&result, len))
    {
        c.rollback();
        c.endTxn();
        c.disconnect();
        exit(1);
    }

    for(int j=0;j<len;j++) {
        for(int i = 0; i<3;i++) 
            printf("%s\t",(char*)result[j][i]);
        printf("\n");
    }
    cout<<"----------------------------------------------------------------"<<endl;
    
    c.endTxn();
    c.disconnect();

    return 0;
}