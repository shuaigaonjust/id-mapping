DROP TABLE IF EXISTS idmapping.index;

CREATE EXTERNAL TABLE IF NOT EXISTS idmapping.index
COMMENT "ID Mapping table for index"
PARTITIONED BY (day string, product string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/compass/public/hive/idmapping/index'
TBLPROPERTIES ( 'avro.schema.url'='/user/compass/public/hive/idmapping/schema/index.avsc' );
