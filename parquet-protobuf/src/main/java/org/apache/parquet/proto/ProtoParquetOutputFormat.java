/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.proto;
import static org.apache.parquet.Log.INFO;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.MemoryManager;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import com.google.protobuf.ExtensionRegistry;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.codec.CodecConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.schema.Type;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.Log;
import org.apache.parquet.hadoop.CodecFactory;
import static org.apache.parquet.hadoop.util.ContextUtil.getConfiguration;
import java.io.IOException;

/**
 * A Hadoop {@link org.apache.hadoop.mapreduce.OutputFormat} for Protocol Buffer Parquet files.
 * <p/>
 * Usage:
 * <p/>
 * <pre>
 * {@code
 * final Job job = new Job(conf, "Parquet writing job");
 * job.setOutputFormatClass(ProtoParquetOutputFormat.class);
 * ProtoParquetOutputFormat.setOutputPath(job, parquetPath);
 * ProtoParquetOutputFormat.setProtobufClass(job, YourProtocolbuffer.class);
 *
 * // to support protobuf extension, the invoker should Override the constructor,
 *   the following is intended pass ExtensionRegistry to ProtoWriteSupport and ProtoSchemaConverter, which need it to convert extension field
 *   class UserProtoParquetOutputFormat<T extends MessageOrBuilder> extends ProtoParquetWriter<T> {
 *       public UserProtoParquetOutputFormat() {
 *         ExtensionRegistry registry = ExtensionRegistry.newInstance();
 *         xx.xx.xx.ClassName.registerAllExtensions(registry);
 *         extensionRegistry = registry;
 *       }
 *   }
 * final Job job = new Job(conf, "Parquet writing job");
 * job.setOutputFormatClass(UserProtoParquetOutputFormat.class);
 * UserProtoParquetOutputFormat.setOutputPath(job, parquetPath);
 * UserProtoParquetOutputFormat.setProtobufClass(job, YourProtocolbuffer);
 * }
 * </pre>
 *
 * @author Lukas Nalezenec
 */
public class ProtoParquetOutputFormat<T extends MessageOrBuilder> extends ParquetOutputFormat<T> {
  private static final Log LOG = Log.getLog(ProtoParquetOutputFormat.class);
  public static ExtensionRegistry extensionRegistry;

  public static void setProtobufClass(Job job, Class<? extends Message> protoClass) {
    ProtoWriteSupport.setSchema(ContextUtil.getConfiguration(job), protoClass);
  }

  public ProtoParquetOutputFormat(Class<? extends Message> msg) {
    super(new ProtoWriteSupport(msg));
  }
  // if need process extension field, then Derived Class should seting extensionRegistry in the Default Constructor
  public ProtoParquetOutputFormat() {
    super(new ProtoWriteSupport());
  }

  @Override
    public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    final Configuration conf = getConfiguration(taskAttemptContext);


    CompressionCodecName codec = getCodec(taskAttemptContext);
    String extension = codec.getExtension() + ".parquet";
    Path file = getDefaultWorkFile(taskAttemptContext, extension);
    return getRecordWriterV2(conf, file, codec);
  }

  private CompressionCodecName getCodec(TaskAttemptContext taskAttemptContext) {
    return CodecConfig.from(taskAttemptContext).getCodec();
  }

  private RecordWriter<Void, T> getRecordWriterV2(Configuration conf, Path file, CompressionCodecName codec)
      throws IOException, InterruptedException {

    final WriteSupport<T> writeSupport = getWriteSupport(conf);
    if (writeSupport instanceof ProtoWriteSupport) {
      ((ProtoWriteSupport)writeSupport).setExtensionRegistry(extensionRegistry);
    }

    CodecFactory codecFactory = new CodecFactory(conf, getPageSize(conf));
    long blockSize = getLongBlockSize(conf);
    if (INFO) LOG.info("Parquet block size to " + blockSize);
    int pageSize = getPageSize(conf);
    if (INFO) LOG.info("Parquet page size to " + pageSize);
    int dictionaryPageSize = getDictionaryPageSize(conf);
    if (INFO) LOG.info("Parquet dictionary page size to " + dictionaryPageSize);
    boolean enableDictionary = getEnableDictionary(conf);
    if (INFO) LOG.info("Dictionary is " + (enableDictionary ? "on" : "off"));
    boolean validating = getValidation(conf);
    if (INFO) LOG.info("Validation is " + (validating ? "on" : "off"));
    WriterVersion writerVersion = getWriterVersion(conf);
    if (INFO) LOG.info("Writer version is: " + writerVersion);
    int maxPaddingSize = getMaxPaddingSize(conf);
    if (INFO) LOG.info("Maximum row group padding size is " + maxPaddingSize + " bytes");

    WriteContext init = writeSupport.init(conf);
    ParquetFileWriter w = new ParquetFileWriter(
        conf, init.getSchema(), file, Mode.CREATE, blockSize, maxPaddingSize);
    w.start();

    float maxLoad = conf.getFloat(ParquetOutputFormat.MEMORY_POOL_RATIO,
                                  MemoryManager.DEFAULT_MEMORY_POOL_RATIO);
    long minAllocation = conf.getLong(ParquetOutputFormat.MIN_MEMORY_ALLOCATION,
                                      MemoryManager.DEFAULT_MIN_MEMORY_ALLOCATION);
    if (memoryManager == null) {
      memoryManager = new MemoryManager(maxLoad, minAllocation);
    } else if (memoryManager.getMemoryPoolRatio() != maxLoad) {
      LOG.warn("The configuration " + MEMORY_POOL_RATIO + " has been set. It should not " +
               "be reset by the new value: " + maxLoad);
    }
    return new ParquetRecordWriter<T>(
        w,
        writeSupport,
        init.getSchema(),
        init.getExtraMetaData(),
        blockSize, pageSize,
        codecFactory.getCompressor(codec),
        dictionaryPageSize,
        enableDictionary,
        validating,
        writerVersion,
        memoryManager);
  }
  private static int getMaxPaddingSize(Configuration conf) {
    // default to no padding, 0% of the row group size
    return conf.getInt(MAX_PADDING_BYTES, DEFAULT_MAX_PADDING_SIZE);
  }
}
