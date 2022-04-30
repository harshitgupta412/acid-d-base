# Acid-D-Base: ToyDB
This project aims to level up toydb to create a better and robust interface along with adding ACID properties.
We first focused on abstracting out the low level C functions that were previously defined in toyDB to create a Object Oriented structure. These classes are responsible for handling communications with the dblayer, amlayer and pflayer. The next task was to define various operations such as querying, project, join etc.
Finally, we created a transaction manager class which will manage all the locks and permissions centrally. Further to provide a good interface to the user, we created the QueryObj and Client classes which creates the query evaluation tree and interact with transaction manager respectively.


## Features
- We implemented a simplistic database engine abstraction which has classes for Database, Tables and Users.
    - For Tables 
        - Tables support addition, deletion and updates of rows
        - It also supports primary key integrity checks. These can be a tuple of columns or a single column 
        - We also support indexing on a key. AMLayer was also modified to allow indexing on multiple columns for indexing on primary keys and others if necessary
        - Tables allow querying based on standard operators like $>, \geq, <, \leq, =$ using existing indexes. We also allow more complex conditions by allowing to pass a custom callback which can evaluate any row to be considered or not.
        - Table allows queries to project table to a subset of columns, take union or intersection of 2 tables or take join of 2 tables.
    - For Databases
        - This is an abstraction that allows creation and deletion of databases, and also for tables within the database
        - This also allows the database to load a table to give a table object from its name
    - For Users 
        - Users are stored in a table with their username and password along with admin status. Only the superuser has admin status, while the other users do not
        - The password of users are stored using a hash function for security
        - Users can also be assigned permissions to read/write a specific database or a table within a database
        - The superuser can create other users and also assign them these permissions
- We have created a daemon process that is constantly running and keeps listening on ports for transaction requests
    - We use sockets to create a server and client side for inter process communications
    - A client can initiate a transaction, perform queries and finally end the transaction, which commits it.
    - The daemon process allows for concurrency of requests with locking at the table level to allow multiple reads and single write
- We also create a simplistic evaluation engine working like 
    - It uses relational algebra as a base to form an evaluation tree structure which is eagerly computed
    - Using this evaluation we can find out which locks are required and use it to implement a Two Phase Locking protocol.


Created BY:
[Dhruv Arora](https://github.com/maverick6130)
    - [Pradipta Bora](https://github.com/geekpradd)
    - [Raj Aryan Agarwal](https://github.com/Anon258)
    - Me 