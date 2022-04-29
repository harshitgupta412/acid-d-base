// #ifndef _QUERYOBJ_H_
// #define _QUERYOBJ_H_

#include<string>
#include<vector>
#include<set>
# define ALL 0
# define EQUAL 1
# define LESS_THAN 2
# define GREATER_THAN 3
# define LESS_THAN_EQUAL 4
# define GREATER_THAN_EQUAL 5
# define NOT_EQUAL 6


enum QueryType {
    Identity_,
    SelectQ_,
    SelectO_,
    Project_,
    Join_,
    Union_,
    Intersection_
};

class QueryObj {
    public:
    std::set<std::string> usedTables;
    QueryType type;
    QueryObj *left;
    QueryObj *right;
    std::string tableName;
    bool (*callback)(int, char*, int);
    std::vector<void*> values;
    int op;
    int indexNo;
    std::vector<int> cols1;
    std::vector<int> cols2;
    QueryObj();

    QueryObj(std::string _tbl);
    QueryObj Select(bool (*callback)(int, char*, int));
    QueryObj Select(int indexNo, int op, std::vector<void*> values);
    QueryObj Project(std::vector<int> cols);
};

QueryObj Join(QueryObj *q1, QueryObj *q2, std::vector<int> cols1, std::vector<int> cols2);
QueryObj Union(QueryObj *q1, QueryObj *q2);
QueryObj Intersection(QueryObj *q1, QueryObj *q2);

// #endif
