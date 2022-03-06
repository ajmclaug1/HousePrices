Normalised SQL Database for Housing Sales Data

Table of contents:

Getting started

Brief overview

Languages and libraries used

Troubleshooting




Getting Started:

The data used is available to download from 

https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

The link to the CSV file in question is 

http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv

It contains information on the sale of every registered house price since 1995.

When setting up MySQL, you will need to create a database called houseprices, you can use the below SQL command.

"create database houseprices;"

Also to ensure you can send the larger datapackets generated whilst importing the data, log in as 
root/user with sufficent clearance and run the below command.

"set global max_allowed_packet=67108864;"

Adding log in information

In the file RunDB.py you will need to add the below information to the config dictionary

config = {'host': 'localhost', 'user': 'test',
          'password': 'testpasswd', 'database': 'houseprices'}

The other files can be left as they are.

When you're ready to go, I'd recommend running RunDB.py from the command line, but feel free to use the IDE of your choice.


Brief overview:

This code is used to create and optimise a MySQL database, it is part of a project which aims to use machine learning to 
predict house prices based on past trends. It will generate generate a relational database with linked foreign key tables
within a Mysql server.

The process took roughly an hour to run on the machines I've tested it on so far, I'd recommend running it from the 
command line once you have added your log in information to the the RunDB.py file.



Languages and libraries Used:
Python:
    pandas
    timeit
    mysql.connector (Install using mysql.connector-python)

SQL:




Troubleshooting:

If you get the below error, ensure that you have installed "mysql.connector-python" not just "mysql.connector"

"mysql.connector.errors.NotSupportedError: Authentication plugin 'caching_sha2_password' is not supported"

If immediately after "HP_Table_Created"  you get an error message similar to the below.

"2055: Lost connection to MySQL server at 'localhost:3306', system error: 1 [SSL: BAD_LENGTH] bad length (_ssl.c:2472)"

Try logging into your server as Root/User with the correct permissions, and run "set global max_allowed_packet=67108864;"

