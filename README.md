## Sparkify AWS Project

### Summary
This code in this repo collects data from S3 buckets, moves them to staging
tables and then transfers them again to into fact and dimension tables.

### How to execute this code

1. Clone the repo
2. Install all requirements in the requirements file
3. Add config file
4. Prestage the data stored in Udacity buckets by executing the prestage_data file
5. Create a Redshift cluster by executing create_cluster.py

    > This assumes you have an IAM Role with the correct access rights.
    > If you don't have this, create an IAM Role and add its ARN to the config file.
6. Create your tables on the Redshift nodes by executing create_tables.py
7. Populate the staging and fact and dimension tables by executing etl.py
8. Delete the cluster using delete_cluster.py

### Staging tables
The staging tables were designed to be one to one replicas of the log and song data
provided. The column names and types correspond accordingly. As Redshift does not
enforce unique or primary key contraints, it is difficult to filter duplicate records out before
copying from the prestaged data files.

### Fact and dimension tables
To get around Redshift's nonenforcement of duplicate entries, SELECT DISTINCT is used to ensure only
unique records are added to the tables. Moreover, WHERE clauses with IS NOT NULL are also used to only
select data from the staging tables that meet the dimension table criteria.
