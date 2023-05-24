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

## QUERY_2)
tblDimDate should have one row (one date) for every date between the first date and the last date in the table. Write a SQL query to determine how many dates are missing, if any,
between the first date and last date. You do not need to supply a list of the missing dates.


## QUERY_3)
Write an SQL query to identify all orders scheduled to run in November 2021, for which there are not yet any records in tblAdvertiserLineItem.


## QUERY_4)
Write an SQL query to count total number of campaigns in tblOrder grouped by campaign duration.
Campaign duration would be the number of days between dateStart and dateEnd.


## QUESTION_1)
Database design:
What are the advantages and disadvantages of creating and using normalized tables?
What are the advantages and disadvantages of creating and using non-normalized tables?
