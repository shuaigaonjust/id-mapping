package com.iflytek.hadoop.idmapping.constants;



public interface ShareConstants {
	public final static String DIR_EXTRACT_DIR = "extract";
	public final static String DIR_GNOME_DIR = "gnome";

	public final static String DIR_EXTRACT_SUBDIR_INTERM = "interm";
	public final static String DIR_EXTRACT_SUBDIR_OUTPUT = "output";
	public final static String DIR_EXTRACT_SUBDIR_TMP = "tmp";

	public final static String DATE_FORMAT_BASIC = "yyyy-MM-dd'T'HH:mm'Z'";
	public final static String DATE_FORMAT_HOURLY_DIR = "yyyy-MM-dd/HH";
	public final static String DATE_FORMAT_DAILY_DIR = "yyyy-MM-dd";
	public final static String DATE_FORMAT_MSEC = "yyyyMMdd-HHmmss-S";
	public final static String DATE_FORMAT_HOURLY = "yyyyMMdd-HH";

	public final static String FLUME_INPUT_FORMAT = "/%Y-%m-%d/%H";
	public final static String REPORT_OOZIE_SUBFLOW = "quickly-report/subflow";

	public final static String EXTRACT_TIME = "extract.time";
	public final static String EXTRACT_BEGINTIME = "extract.begintime";

	public final static String INPUT_FILE_TYPE_AVRO = "avro";

	public final static String INPUT_FILE_TYPE_JSON = "json";
	public final static String INPUT_FILE_TYPE_SEQFILE = "seqfile";
	public final static String INPUT_FILE_TYPE_SEQJSON = "seqjson";
	public final static String OPERATION_CLEAN = "clean";
	public final static String OPERATION_EXTRACT = "extract";

	public final static String INPUT_FILE_TYPE_TXTLINE = "text";
	public final static String RUN_TYPE_GROOVY = "groovy";
	public final static int APPID_VOICECLOUD = 1;
	public final static int APPID_CALLER = 2;
	public final static String APPID_PACKAGE = "packagename";
	public final static String INITCONFIGPATH = "initconfigpath";
	public final static String CLEANMETHODSNAME = "cleanmethodsname";

	public final static int APPID_SDK = 3;
	public final static int APPID_AD = 4;
	public final static String RUN_TYPE_JAVA = "java";
	public final static String RUN_TYPE = "extract.run.type";
	public final static String GROOVY_PATH = "extract.groovy.path";
	public static final String EXTRACT_ALGORITHMS = "extract.algorithm";
	public static final String EXTRACT_DATATYPE = "extract.datatype";
	public static final String EXTRACT_DATANAME = "extract.dataname";
	public static final String EXTRACT_OPERATION = "extract.operation";
	public static final String EXTRACT_ISTODID = "extract.istodid";
	public static final String EXTRACT_MYSQL = "extract.mysql";
	public static final String DEVICE_IMEI = "imei";
	public static final String DEVICE_MAC = "mac";

	public static final String DEVICE_IDFA = "idfa";

	public static final String DEVICE_OPENUDID = "openudid";

	public static final String COUNT_REPORT = "countreport";
	public static final String GROUP_UD_COUNTERS = "User-defined Counters";
	public static final String DUPLICATE_FLAG = "Duplicate_removal_flag";

	public static final String COUNT = "statistic";

	/* 鐢ㄤ簬鏃ュ織瀛楁缁熶竴鍖栫殑涓�簺甯搁噺 */
	public final static String STR_EMPTY = "EMPTY";
	public final static String STR_UNKNOWN = "UNKNOWN";
	public final static double DOU_EMPTY = Float.MIN_VALUE;
    public final static String SCHEMA_STR = "{\"type\" : \"record\",\"name\" : \"ids\",\"fields\" : [{\"name\" : \"global_id\" ,\"type\" : \"string\" },{\"name\" : \"imei\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"imei\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}} },{\"name\" : \"uid\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"uid\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}},{\"name\" : \"mac\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"mac\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}},{\"name\" : \"idfa\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"idfa\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}},{\"name\" : \"openudid\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"openudid\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}},{\"name\" : \"imsi\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"imsi\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}},{\"name\" : \"phone_num\" ,\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"phone_num\",\"fields\" : [{\"name\":\"value\", \"type\":\"string\"},{\"name\":\"activity\", \"type\":\"string\"}]}}}]}";

    public final static String ID = "ID";
}












