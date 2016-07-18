DROP TABLE IF EXISTS idmapping.ids;

CREATE EXTERNAL TABLE IF NOT EXISTS idmapping.ids
COMMENT "ID Mapping table for ids"
PARTITIONED BY (product string, day string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/compass/public/hive/idmapping/ids'
TBLPROPERTIES ( 'avro.schema.url'='/user/compass/public/hive/idmapping/schema/ids.avsc' );
