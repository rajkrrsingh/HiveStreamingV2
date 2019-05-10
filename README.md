# HiveStreamingV2
This application demonstrate the use of HiveStreaming v2(Hive3), the application is using static partitioning to write to hive acid table.

more information can be found here :- [https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest+V2]

#### Create table with following Schema upfront
```   create table alerts ( id int , msg string )
        partitioned by (continent string, country string)
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



[]: https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest+V2