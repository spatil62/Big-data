# Week-05 Lab

## Objectives

* Create and demonstrate the creation of Spark Schemas for Python and Scala
* Create and build running Spark Applications
* Create and build an application that will read arbitrary input

## Outcomes

At the conclusion of this lab you will have built Spark applciations in Python and Scala and demonstrated the three methods for assigning schemas to arbitrary data.

## Assignment Setup

- Using the file `Divvy_Trips_2015-Q1.zip`, extract the CSV file provided
- In your local repo add the following data types to the `.gitignore` file
  - `*.csv`
  - `*.txt`
  - `*.json`
  - `target/` to ignore Jar file artifacts
- You can extract a compressed Zip file using the command: `unzip Divvy_Trips_2015-Q1.zip`
  - May have to use `sudo apt-get install unzip` if application not found
- Complete the assignment and push this Readme.md template to the remote repo.

## Assignment Details

- In your local Git Repo, create a folder named **labs** under the folder and create a sub-folder **week-05** and place this Readme.md template in that directory along with the required pieces of code
  - You will create two programs, one in Python and one in Scala
    - Python: week-05.py
    - Scala: week-05.scala
  - Per lanugage file, create three data frames reading the extracted `Divvy_Trips_2015-Q1.csv` from your local system
    - First **infer the schema** and read the csv file
    - Second programmatically use **StructFields** to create and attach a schema and read the csv file
    - Third attach a schema via a DDL and read the csv file
      
- After each read operation include the function `printSchema()`

- Also use the `.count()` function to display the number of records in each DataFrame
  
### printSchema inferred in Python


### printSchema programmatically in Python


### printSchema via DDL in Python


### printSchema inferred in Scala


### printSchema programmatically in Scala


### printSchema via DDL in Scala



