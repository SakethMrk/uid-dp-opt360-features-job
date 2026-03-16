package com.foo.bar.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.foo.bar.dto.OutMessage;
import com.foo.bar.schema.kafka.OutMessageSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.apache.kafka.clients.producer.ProducerConfig;

@Slf4j
public class Sinks {

	public void prepareSink(DataStream<Row> rowStream) {

		String HIVE_CATALOG = "iceberg_catalog";

		HiveCatalog catalog = new HiveCatalog();
		catalog.initialize("hive", hiveProperties());

		CatalogLoader catalogLoader = CatalogLoader.hive(HIVE_CATALOG, hadoopConfigs(), hiveProperties());

		Schema schema = new Schema(
				Types.NestedField.optional(1, "entity_id", Types.StringType.get()),
				Types.NestedField.optional(2, "feature_id", Types.StringType.get()),
				Types.NestedField.optional(3, "feature_name", Types.StringType.get()),
				Types.NestedField.optional(4, "feature_version", Types.StringType.get()),
				Types.NestedField.optional(5, "feature_value", Types.DoubleType.get()),
				Types.NestedField.optional(6, "timestamp", Types.TimestampType.withoutZone()),
				Types.NestedField.optional(7, "comments", Types.StringType.get())
		);

//		Schema schemaDevice = new Schema(
//				Types.NestedField.optional(1, "opt_id", Types.StringType.get()),
//				Types.NestedField.optional(2, "previous_devicecode_last_ts", Types.TimestampType.withoutZone()),
//				Types.NestedField.optional(3, "previous_devicecode_txns", Types.LongType.get()),
//				Types.NestedField.optional(4, "previous_devicecode", Types.StringType.get()),
//				Types.NestedField.optional(5, "current_devicecode", Types.StringType.get()),
//				Types.NestedField.optional(6, "current_auth_type", Types.StringType.get()),
//				Types.NestedField.optional(7, "ets", Types.TimestampType.withoutZone())
//		);

		TableIdentifier outputTable1 = TableIdentifier.of(Driver.Configurations.DATABASE, "opt_features_auth_v1");
//		TableIdentifier outputTable2 = TableIdentifier.of(Driver.Configurations.DATABASE, "opt_auth_device_change_v1");

//		if (!catalog.tableExists(outputTable1)) {
//			catalog.createTable(outputTable1, schema, PartitionSpec.builderFor(schema)
//					.bucket("entity_id",32)
//							.identity("feature_name")
//					.build());
//		}

//		if (!catalog.tableExists(outputTable2)) {
//			catalog.createTable(outputTable2, schemaDevice, PartitionSpec.builderFor(schemaDevice).truncate("opt_id", 2).build());
//		}

		TableLoader tableLoader1 = TableLoader.fromCatalog(catalogLoader, outputTable1);
//		TableLoader tableLoader2 = TableLoader.fromCatalog(catalogLoader, outputTable2);

		if(rowStream!=null)
			FlinkSink.forRow(rowStream, FlinkSchemaUtil.toSchema(schema))
					.tableLoader(tableLoader1)
					.writeParallelism(4)
					.equalityFieldColumns(List.of("feature_id","entity_id","timestamp"))
					.distributionMode(DistributionMode.HASH)
					.overwrite(false)
					.upsert(true)
					.set("write.target-file-size-bytes","536870912")
					.append();

//		if(rowStreamDevice!=null)
//			FlinkSink.forRow(rowStreamDevice, FlinkSchemaUtil.toSchema(schemaDevice))
//					.tableLoader(tableLoader2)
//					.writeParallelism(4)
//					.equalityFieldColumns(List.of("opt_id", "previous_devicecode_last_ts"))
//					.distributionMode(DistributionMode.HASH)
//					.overwrite(false)
//					.upsert(true)
//					.set("write.target-file-size-bytes","536870912")
//					.append();
	}


	private Map<String, String> hiveProperties() {
		Map<String, String> properties = new HashMap<>();

		properties.put("type", "iceberg");
		properties.put("connector", "iceberg");
		properties.put("catalog-name", Driver.Configurations.CATALOG);
		properties.put("catalog-type", "hive");

		properties.put("catalog-database", Driver.Configurations.DATABASE);

		properties.put("clients", "5");
		properties.put("property-version", "2");
		properties.put("warehouse", Driver.Configurations.WAREHOUSE);

		properties.put("s3.endpoint", Driver.Configurations.S3ENDPOINT);
		properties.put("uri", Driver.Configurations.THRIFT_URI);
		properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

		return properties;
	}
	public KafkaSink<OutMessage> createKafkaSink(String outTopic, String kafkaBroker) {
		return KafkaSink.<OutMessage>builder()
				.setBootstrapServers(kafkaBroker)
				.setRecordSerializer(new OutMessageSerializer(outTopic))
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.setTransactionalIdPrefix("tx-auth-features-test-prefix-")
				.setProperty("request.timeout.ms", "140000")
				.setProperty("delivery.timeout.ms", "150000")
				.setProperty("batch.size", "8192")
				.setProperty("linger.ms", "10")
				.setProperty(ProducerConfig.ACKS_CONFIG , "all")
				.build();
	}

	private Configuration hadoopConfigs() {
		return new Configuration();
	}
}