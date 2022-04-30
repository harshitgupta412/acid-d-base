rm -rf temp/;
mkdir temp/;

cd pflayer/;
make clean;make;
cd ../amlayer/;
make clean;make;
cd ../dblayer/;
make clean;make;

cd ../;
echo $PWD;
make clean;make;

make load_data;
make test_query;
make join_query;
make test_query_index;
make add_delete;
make add_delete_table;
make indexes;
make user_test;