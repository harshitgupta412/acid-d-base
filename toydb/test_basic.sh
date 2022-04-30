read -p "Press enter to test load_data " a;
./tests/load.bin;
read -p "Press enter to test callback query " a;
./tests/query.bin;

read -p "Press enter to test join query " a;
./tests/join.bin;

read -p "Press enter to test index query " a ;
./tests/query_index.bin;

read -p "Press enter to add delete rows" a ;
./tests/add_delete.bin;

read -p "Press enter to test add delete table" a;
./tests/add_delete_table.bin;

read -p "Press enter to test indexes" a;
./tests/indexes.bin;

read -p "Press enter to test user permission" a;
./tests/user_permission.bin;

read -p "Press enter to test query_union" a;
./tests/query_union.bin;

read -p "Press enter to test query_project" a;
./tests/query_project.bin;

read -p "Press enter to test query_intersect" a;
./tests/query_intersect.bin;