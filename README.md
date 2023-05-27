# data_challenge


# Code Test - Data Operations
- Table of Contents
- SQL 1
- Hive, HDFS 4
- Python 5
- Linux, Bash 



#  SQL
You have a database with the following tables:
- **tblDimDate** - a table of dates, i.e., a calendar
- **tblOrder** - a table of 'Orders', also referred to as Campaigns
- **tblAdvertiserLineItem** - a table of 'Advertiser Line Items' (ALI for short).
Each ALI is a component of a campaign.
Therefore, the relation of tblAdvertiserLineItem to tblOrder is many-to-one, with the foriegn key relationship described below.
Use the sample data and schema descriptions below to provide the following queries:


## QUERY_1)
Write an SQL query to return all months in the current year for which there are exactly 30 days.
```sql
WITH
  random_data_cte AS (
  SELECT
    CAST((EXTRACT(YEAR
        FROM
          CURRENT_DATE()) || '-' || LPAD(CAST(ROW_NUMBER() OVER () AS STRING),2,'0') || '-' || '01') AS DATETIME) AS gen_data
  FROM
    UNNEST(GENERATE_ARRAY(1, 12)) )
SELECT
  EXTRACT(MONTH
  FROM
    LAST_DAY(gen_data)) AS month,
  EXTRACT(DAY
  FROM
    LAST_DAY(gen_data)) AS max_day
FROM
  random_data_cte
WHERE
  EXTRACT(DAY
  FROM
    LAST_DAY(gen_data)) =30

```

PS: Answered this before reading the whole document

```sql
SELECT
  iMonth,
  sMonth
FROM
  tblDimDate
WHERE
  iYear=EXTRACT(YEAR
  FROM
    CURRENT_DATE())
  AND EXTRACT(DAY
  FROM
    LAST_DAY(dateDay))=30
```

## QUERY_2)
tblDimDate should have one row (one date) for every date between the first date and the last date in the table. Write a SQL query to determine how many dates are missing, if any,
between the first date and last date. You do not need to supply a list of the missing dates.

```sql
WITH
  max_min_dates AS (
  SELECT
    MIN(DATE(dateDay)) AS minDate,
    MAX(DATE(dateDay)) AS maxDate
  FROM
    tblDimDate),
  data_range_cte AS (
  SELECT
    CAST(data_range AS STRING) AS data_range
  FROM
    max_min_dates,
    UNNEST(GENERATE_DATE_ARRAY(minDate, maxDate)) AS data_range )
SELECT
  COUNT(data_range)
FROM
  data_range_cte
WHERE
  data_range NOT IN (
  SELECT
    dateDay
  FROM
    cte_dates)

```

## QUERY_3)
Write an SQL query to identify all orders scheduled to run in November 2021, for which there are not yet any records in tblAdvertiserLineItem.

```sql
SELECT
  *
FROM
  tblOrder
LEFT OUTER JOIN
  tblAdvertiserLineItem
ON
  tblOrder.id = tblAdvertiserLineItem.idOrder

```

## QUERY_4)
Write an SQL query to count total number of campaigns in tblOrder grouped by campaign duration.
Campaign duration would be the number of days between dateStart and dateEnd.

```sql
SELECT
  DATE_DIFF(dateEnd, dateStart, DAY) AS cp_duration,
  COUNT(id)
FROM
  tblOrder
GROUP BY
  1
```

## QUESTION_1)
Database design:
What are the advantages and disadvantages of creating and using normalized tables?
```md
Normalization helps eliminate data redundancy by breaking down information into smaller atomic units, improving data integrity and reducing inconsistencies, leading to more efficient storage and processing overhead. Normalization also allows for more flexible querying and analysis, as data is separated into logical entities, enabling more specific and targeted queries.

It's also good to remember that certain BI, like Tableau, take advantage of this kind of normalization.

As disadvantges, normalization can increase the complexity of queries and joins, especially in complex table relationships. A team can easily get lost working with undocumented facts/dimensions tables, making governance and documentation a must. 

- a case scenario for this would be a datawarehouse

```
What are the advantages and disadvantages of creating and using non-normalized tables?

```md

Non-normalized tables often have a flat structure, which means there is less complexity in terms of relationships and joins between tables. This simplicity can make it easier to understand and work with the data, especially in smaller databases or for simple applications.

Non-normalized(flat) tables, on a columnar storage, can be as performative or even more than normalized.


As a disadvantge, flat tables are prone to have data redundancy and inconsistencies

- a case scenario for this would be a datalake

```

# Hive, HDFS
QUESTION_1) Given a table in Hive, how do we identify where the data is stored?

```sql
DESCRIBE table_name;

SHOW CREATE TABLE table_name;

```

QUESTION_2) How can we see what partitions the table may have?

```hql

SHOW PARTITIONS table_name;


```

QUESTION_3) How can we see if the table is missing any partitions?

```hql

MSCK REPAIR TABLE table_name;

```


Given a table, 'auctions' with the following DDL:
```sql
CREATE TABLE auctions (
  auctionid string COMMENT 'the unique identifier for the auction',
  idlineitem bigint COMMENT 'the Line Item ID associated with the auction',
  arysegments array<string> COMMENT 'the array of segments associated with the auction'
)
PARTITIONED BY (utc_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/bidder_data/auctions' ;

```

What would be the queries to answer the following questions:
## QUERY_1) Provide an HQL query to provide a distribution of the number of auctions and line items, grouped by the number of segments within each auction record.

```sql
SELECT
  size(arysegments) AS num_segments,
  COUNT(DISTINCT auctionid) AS num_auctions,
  COUNT(idlineitem) AS num_line_items
FROM
  auctions
GROUP BY
  1;

```
## QUERY_2) Provide an HQL query to provide the distinct count of auctions and line items, associated to each segment within arysegments. (HINT: You will need to expand the
segment array first.)


```sql
SELECT
  segment,
  COUNT(DISTINCT auctionid) AS num_auctions,
  COUNT(idlineitem) AS num_line_items
FROM
  auctions LATERAL VIEW explode(arysegments) t AS segment
GROUP BY
  1;

```


# Linux, Bash
You need to run a hive query for a sequence of 30 dates (all dates in September 2021)..
The size of the table data and cluster bandwidth require that the query be run for each date individually.
The query string will accept 'DATE' as an argument, as follows:
HQL_QUERY="select utc_date, sum(1) as num_rows from my_table where utc_date = '${DATE}' group by utc_date"
Using bash, write a script which will execute this query for all dates in September 2021 and store the result to a single file.
The file will have 2 columns and up to 30 rows.

```bash
#!/bin/bash

for day in {1..30}; do
    date=$(printf "2021-09-%02d" "$day")

    hql_query="select utc_date, sum(1) as num_rows from my_table where utc_date='${DATE}' group by utc_date"

    hive -e "$hql_query" >> "out.txt"
done

```

# Python

You need to run a hive query for a sequence of 30 dates (all dates in September 2021)..
The size of the table data and cluster bandwidth require that the query be run for each date individually.
The hive query will accept 'DATE' as an argument, as follows.
HQL_QUERY = "select utc_date, sum(1) as num_rows from my_table where utc_date = '${DATE}' group by utc_date"
Using Python, write a script which will execute this query for all dates in September 2021 and store the result to a single file.
The file will have 2 columns and up to 30 rows.

```python

import subprocess

hql_query = "select utc_date, sum(1) as num_rows from my_table where utc_date='${DATE}' group by utc_date"
dates = ['2021-09-{0:02d}'.format(day) for day in range(1, 31)]

results = []
for date in dates:
    query = hql_query.replace('${DATE}', date)
    command = f'hive -e "{query}"'
    output = subprocess.check_output(command, shell=True).decode('utf-8')
    data = output.strip().split('\t')[1:]
    results.append([date, data[0]])

with open('out.txt', 'w') as f:
    f.write('utc_date,num_rows\n')
    for result in results:
        f.write(','.join(result) + '\n')

```