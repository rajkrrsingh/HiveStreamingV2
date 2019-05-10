package com.rajkrrsingh.test.hivestreaming;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.streaming.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 *
 * create table with following schema
 *
 * create table students ( id int , name string )
 *      partitioned by (year string, month string)
 *      clustered by (id) into 2 buckets
 *      stored as orc tblproperties("transactional"="true");
 *
 */
public class HiveStreamingV3{

    private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingV3.class.getName());

    private static String METASTORE_URI;
    private static String DB_NAME;
    private static String TABLE_NAME;
    private static List<String> PARTITION_VALUES;
    private static final String CLIENT_CACHE_DISABLED_PROPERTY = "hcatalog.hive.client.cache.disabled";

    public static void main( String[] args )
    {
        HiveConf conf = new HiveConf();
        conf.setBoolean(CLIENT_CACHE_DISABLED_PROPERTY,true);

        Yaml yaml = new Yaml();
        InputStream inputStream = HiveStreamingV3.class
                .getClassLoader()
                .getResourceAsStream("hive-streaming.yaml");
        Map<String, Object> props = yaml.load(inputStream);

        METASTORE_URI = props.get("METASTORE_URI").toString();
        DB_NAME = props.get("DB_NAME").toString();
        TABLE_NAME = props.get("TABLE_NAME").toString();
        if(props.get("PARTITION_VALUES")!= null){
            PARTITION_VALUES = (List<String>) props.get("PARTITION_VALUES");
        }

        UserGroupInformation ugi = null;
        StreamingConnection hiveStreamingConnection = null;
        try {

            HiveOptions hiveOptions = new HiveOptions(METASTORE_URI,DB_NAME,TABLE_NAME)
                    .withHiveConf(conf)
                    .withAutoCreatePartitions(true)
                    .withCallTimeout(10000);

            if(!PARTITION_VALUES.isEmpty())  {
                hiveOptions.withStaticPartitionValues(PARTITION_VALUES);
            }

            if ("KERBEROS".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION))) {

                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL).trim(), HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB).trim());
                ugi = UserGroupInformation.getCurrentUser();
                hiveOptions.withKerberosPrincipal(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL).trim())
                        .withKerberosKeytab(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB).trim());
            }
            StrictDelimitedInputWriter writer = StrictDelimitedInputWriter.newBuilder()
                    .withFieldDelimiter(',')
                    .build();
            LOG.info("Going to create streaming connection");
            hiveStreamingConnection = makeStreamingConnection(hiveOptions,writer);

            LOG.info("About to write few records in db: "+DB_NAME+" table : "+TABLE_NAME);
            hiveStreamingConnection.beginTransaction();
            hiveStreamingConnection.write("1,val1".getBytes());
            hiveStreamingConnection.write("2,val2".getBytes());
            hiveStreamingConnection.commitTransaction();

            hiveStreamingConnection.beginTransaction();
            hiveStreamingConnection.write("3,val3".getBytes());
            hiveStreamingConnection.write("4,val4".getBytes());
            hiveStreamingConnection.commitTransaction();
            LOG.info("Able to write few records succesfully");
            System.out.println("Able to write few records in "+DB_NAME+"."+TABLE_NAME+" succesfully");
        }catch (Exception e){
            LOG.error("Aborting Txn because of an Exception "+e.getMessage());
            try {
                hiveStreamingConnection.abortTransaction();
            } catch (StreamingException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            if(inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(hiveStreamingConnection != null){
                hiveStreamingConnection.close();
            }
        }
    }

    private static StreamingConnection makeStreamingConnection(HiveOptions options, RecordWriter writer) throws StreamingException {
        return HiveStreamingConnection.newBuilder()
                .withDatabase(options.getDatabaseName())
                .withTable(options.getTableName())
                .withStaticPartitionValues(options.getStaticPartitionValues())
                .withHiveConf(options.getHiveConf())
                .withRecordWriter(writer)
                .withAgentInfo("TEST_AGENT")
                .connect();
    }



}

