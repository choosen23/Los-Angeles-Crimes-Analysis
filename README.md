# Big Data Project

| AM | 03003148 |
| --- | --- |
| email | https://www.notion.somailto:%66%69l%69%70pou%2e%6d%69ch%61%69l@%67%6d%61%69l%2e%63%6f%6d |
| github |  |
| Name | Filippou Michail |

## Requirement 1

| Name | Link |
| --- | --- |
| Spark | http://83.212.72.191:8080/ |
| Hadoop | http://83.212.72.191:9870/dfshealth.html#tab-overview |

## Requirement 2

The commands to upload the files to hadoop

```json
hadoop fs -mkdir -p ~/data

hadoop fds-put la_crimes_1.csv la_crimes_2.csv LA_income_2015.csv LAPD_Police_Stations.csv revgecoding.csv ~/data

```

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled.png)

## Requirement 3

### Results

+----+-----+-----------+-------+
|year|month|crime_total|ranking|
+----+-----+-----------+-------+
|2010|1    |19520      |1      |
|2010|3    |18131      |2      |
|2010|7    |17857      |3      |
|2011|1    |18141      |1      |
|2011|7    |17283      |2      |
|2011|10   |17034      |3      |
|2012|1    |17954      |1      |
|2012|8    |17661      |2      |
|2012|5    |17502      |3      |
|2013|8    |17441      |1      |
|2013|1    |16828      |2      |
|2013|7    |16645      |3      |
|2014|10   |17331      |1      |
|2014|7    |17258      |2      |
|2014|12   |17198      |3      |
|2015|10   |19221      |1      |
|2015|8    |19011      |2      |
|2015|7    |18709      |3      |
|2016|10   |19660      |1      |
|2016|8    |19496      |2      |
|2016|7    |19450      |3      |
|2017|10   |20437      |1      |
|2017|7    |20199      |2      |
|2017|1    |19849      |3      |
|2018|5    |19976      |1      |
|2018|7    |19879      |2      |
|2018|8    |19765      |3      |
|2019|7    |19126      |1      |
|2019|8    |18987      |2      |
|2019|3    |18865      |3      |
|2020|1    |18542      |1      |
|2020|2    |17273      |2      |
|2020|5    |17221      |3      |
|2021|10   |19328      |1      |
|2021|7    |18673      |2      |
|2021|8    |18389      |3      |
|2022|5    |20453      |1      |
|2022|10   |20315      |2      |
|2022|6    |20257      |3      |
|2023|10   |20040      |1      |
|2023|8    |20029      |2      |
|2023|1    |19908      |3      |
|2024|1    |18772      |1      |
|2024|2    |17244      |2      |
|2024|3    |16074      |3      |
+----+-----+-----------+-------+

[Times per Method](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Times%20per%20Method%2067983ac292184dbcbb1b23113564f2a5.csv)

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%201.png)

## Requirement 4

### Results

+---------+------+
|part_day |count |
+---------+------+
|Night    |243815|
|Evening  |192164|
|Afternoon|151816|
|Morning  |126865|
+---------+------+

[Times per Approach](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Times%20per%20Approach%203f9a4b7f510249508b6860f3a4c0726d.csv)

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%202.png)

## Requirement 5

Before answering the queries lets understand some things.

**Catalyst Optimizer**

A novel query optimizer for Spark using advance techniques like pattern matching etc. [[1](https://www.databricks.com/glossary/catalyst-optimizer)]

**Join Strategy.**

Based on the factors of the data, like data size, data distribution, if there are and how are joined, the format, the spark selects different methods to do joins the data.

1. Broadcast Join

Used when one of the data set is small enough to fit in memory, then is broadcasted to all the worker nodes.

**Cases**

- One dataset is significantly smaller than the other.
- Only perform equi-join.
- No bandwidth constraints.

1. Shuffle Join

A common strategy to join two large datasets.

**Cases**

- Too large data to fit in one node.
- Data are partitioned across multiple nodes.
- Outer or left outer joins in the large data sets.

1. Sort Merge Join

Joins large data sets that are spread across multiple nodes in the cluster.

**Cases**

- Large data, but not so large to user shuffle join.
- Not evently distributed across platforms.
- Operations that cannot perform using broadcast joins (full outer join, left outer join)

1. Broadcast Nested Loop

One small (to fit in a node) and one larger. The smaller broadcasted to all the nodes.

**Cases**

- One dataset fits the memory
- Selective join key, small number of matching records.
- Large dataset has a good data distribution (not scattered).

Explanations from [here](https://python.plainenglish.io/a-deep-dive-into-the-inner-workings-of-apache-spark-join-strategies-5d47dd29a4cb)!

## Results

For the top 3 incomes:

+--------------------+-------------+
|        Vict Descent|total_victims|
+--------------------+-------------+
|Hispanic/Latin/Me...|         1996|
|               White|         1136|
|               Black|          952|
|               Other|          518|
|             Unknown|          287|
|         Other Asian|           93|
|            Filipino|           10|
|              Korean|            9|
|American Indian/A...|            4|
|             Chinese|            2|
|            Japanese|            2|
|          Vietnamese|            1|
|           Guamanian|            1|
+--------------------+-------------+

For the bottom 3 incomes:

+--------------------+-------------+
|        Vict Descent|total_victims|
+--------------------+-------------+
|               White|            5|
|Hispanic/Latin/Me...|            4|
|             Unknown|            3|
|               Black|            3|
|               Other|            1|
+--------------------+-------------+

Using the command :

```json
top_3_crimes.explain(mode="extended")
```

We can have the details about the join.

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%203.png)

The optimizer uses BroadcastHashJoin.

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%204.png)

And for the last query uses SortMergeJoin

[Join Strategies](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Join%20Strategies%20e6d92ff37eca44c2a3760c994945d06a.csv)

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%205.png)

As expected, Merge Join performed very well, due to the fact that our datasets are easily sorted.

The reason the shuffle hast performed so well, is due to the fact that the distribution of the join keys, suits this method.

We expected the broadcast join to perform better due to the fact that we joined a small and a large datasets, and as it mentioned this is the ideal join strategy.

Shuffle Replicate, needs enormous resources in order to work with large files, so it is expected to perform worst.

**Screenshot from the history** 

![Untitled](Big%20Data%20Project%20fa36836d700d4e99ae874b9e82bf68ff/Untitled%206.png)

## Requirement 6

### Result

+-----------+------------------+---------------+
|division    |average_distance  |incidents_total|
+-----------+------------------+---------------+
|77th Street|8341.568622849469 |17021          |
|Southeast  |12937.589629958506|12948          |
|Newton     |6213.464524257617 |9844           |
|Southwest  |15851.752251094427|8912           |
|Hollenbeck |13553.628977447854|6202           |
|Harbor     |15769.635432110937|5622           |
|Rampart    |5640.65311302953  |5116           |
|Mission    |14802.533072226992|4503           |
|Olympic    |16365.801498962444|4424           |
|Northeast  |6857.946290753058 |3920           |
|Foothill   |10373.145862441319|3775           |
|Hollywood  |13279.880232264879|3643           |
|Central    |13782.2106740909  |3615           |
|Wilshire   |7613.386922339974 |3525           |
|N Hollywood|11774.735196819262|3465           |
|West Valley|10043.938742117367|2903           |
|Van Nuys   |10873.477689439418|2733           |
|Pacific    |7889.32894337054  |2709           |
|Devonshire |17242.142869989486|2472           |
|Topanga    |7494.3508454559   |2285           |
+-----------+------------------+---------------+

### Analysis

We will provide a pseudocode for each one of the approaches.

**Repetition Join**

1. In the map phase, each map task processes a split of either Crime Data (R), or  (S) from Stations.
2. Union the datasets.
3. Repartitioned by the ‘AREA’ key, preparing for the join operation.
4. Sorting each partition by ‘AREA’
5. Grouping the by the ‘AREA’ key.
6. Separate the records based on the ‘S’ or ‘R’.  
    1. We have for each AREA the police stations coordinates and all the crimes that are marked with the same area.
7. Cross-Product Join. We find for each crime the distance from the police station and the count of the crime.

So we have for each area the total crimes and the distance from the station in the format of 

**<AREA, distance, 1>** 

**Broadcast Join**

1. Create a Map of the station data.
2. Broadcast it to all Spark Nodes.
3. Define the Join Function.
    - For each row get the broadcasted area.
    - If there is the station in the broadcasted data,
    - Calculate the distance between this station and the crime geolocation, and return it with the counter. <AREA, DISTANCE, 1>
4. Apply this to each record to each Node.
5. Return the result.

## Requirement 7

### Results

Look Requirement 6.

We implement the hole feature in the previous Requirement.