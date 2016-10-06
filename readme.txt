
hbase(main):009:0> create_namespace 'google'

hbase(main):009:0> create 'google:employee','emp'


File is stored in below directory
---------------------------------
/home/cloudera/employee


Run the Java Program

hbase(main):009:0> scan 'google:employee'


Also: this project has got couple of program
---------------------------------------------

1. Hadoop row count of table
2. Export NYSE dataset (CVS file format)

source code is maintained in my GitHub

https://github.com/sujith-es/HBase
