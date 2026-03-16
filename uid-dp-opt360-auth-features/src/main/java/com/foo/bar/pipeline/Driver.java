package com.foo.bar.pipeline;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Driver {
	public static void main(String[] args) throws Exception {
		FlinkPipeline pipeline = new FlinkPipeline();
		pipeline.compose();
	}
	public static class Configurations {
		public static final String KAFKA_BOOTSTRAP_SERVERS = "10.10.107.67:9092,10.10.106.91:9092,10.10.107.133:9092,10.10.107.103:9092,10.10.107.112:9092,10.10.107.177:9092,10.10.107.105:9092";
		public static final String KAFKA_TOPIC = "DE.OPT.AUTH_EVENTS.V1";
		public static final String KAFKA_OUTPUT_TOPIC = "DE.OPT.AUTH.FEATURES.TEST";
		static final String THRIFT_URI = "thrift://10.10.118.15:9083";
		static final String WAREHOUSE = "prd-bi-data-platform-results";
		static final String CATALOG = "flink_stream";
		static final String DATABASE = "temp";
		static final String S3ENDPOINT = "http://10.10.103.12:425";

		static final long CHECK_POINT_INTERVAL = 120000L;
		static final String CHECK_POINT_STORAGE = "s3a://prd-bi-data-platform-configs/flink/checkpoints/";

		public static final ZoneId istZone = ZoneId.of("Asia/Kolkata");
		public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
	}}