package com.foo.bar.pipeline;

import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import java.util.Collection;

public class Options implements RocksDBOptionsFactory {

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setIncreaseParallelism(4);
        return currentOptions;
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setLevel0FileNumCompactionTrigger(4);
        currentOptions.setCompactionStyle(CompactionStyle.LEVEL);

        currentOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        currentOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);

        currentOptions.setWriteBufferSize(64 * 1024 * 1024);
        currentOptions.setTargetFileSizeBase(64 * 1024 * 1024);

        return currentOptions;
    }
}