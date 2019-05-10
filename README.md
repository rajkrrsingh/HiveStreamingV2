# HiveStreamingV2
This application devloped and tested on HDP-3.1.0.0-78 and demonstrate the use of HiveStreaming v2(Hive3), the application is using static partitioning to write to hive acid table.

more information can be found here :- [https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest+V2]

#### Create table with following Schema upfront
```   
create table students ( id int , name string )
        partitioned by (year string, month string)
          clustered by (id) into 2 buckets
           stored as orc tblproperties("transactional"="true");
```

#### Build
``
mvn clean package
``

#### Run 
to run this application make sure that table has been created successfully and use hive user to run the app.


```$xslt
su - hive
java -cp .:/tmp/HiveStreamingV3-1.0-SNAPSHOT.jar:/usr/hdp/3.1.0.0-78/hive/lib/*:`hadoop classpath` com.rajkrrsingh.test.hivestreaming.HiveStreamingV3```


```

#### Result
```$xslt
 hive -e "select * from students"
 +--------------+----------------+----------------+-----------------+
 | students.id  | students.name  | students.year  | students.month  |
 +--------------+----------------+----------------+-----------------+
 | 2            | val2           | 2018           | 12              |
 | 1            | val1           | 2018           | 12              |
 | 3            | val3           | 2018           | 12              |
 | 4            | val4           | 2018           | 12              |
 +--------------+----------------+----------------+-----------------+

```