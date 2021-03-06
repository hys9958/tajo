/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.text.ByteBufLineReader;
import org.apache.tajo.storage.text.DelimitedLineReader;
import org.apache.tajo.storage.text.DelimitedTextFile;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestLineReader {
	private static String TEST_PATH = "target/test-data/TestLineReader";

  @Test
  public void testByteBufLineReader() throws IOException {
    TajoConf conf = new TajoConf();
    Path testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    FileSystem fs = testDir.getFileSystem(conf);

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("comment", Type.TEXT);
    schema.addColumn("comment2", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.TEXTFILE);
    Path tablePath = new Path(testDir, "line.data");
    FileAppender appender = (FileAppender) StorageManager.getStorageManager(conf).getAppender(meta, schema,
        tablePath);
    appender.enableStats();
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;

    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(25l));
      vTuple.put(2, DatumFactory.createText("emiya muljomdao"));
      vTuple.put(3, NullDatum.get());
      appender.addTuple(vTuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);

    ByteBufInputChannel channel = new ByteBufInputChannel(fs.open(tablePath));
    assertEquals(status.getLen(), channel.available());
    ByteBufLineReader reader = new ByteBufLineReader(channel);
    assertEquals(status.getLen(), reader.available());

    long totalRead = 0;
    int i = 0;
    AtomicInteger bytes = new AtomicInteger();
    for(;;){
      ByteBuf buf = reader.readLineBuf(bytes);
      if(buf == null) break;

      totalRead += bytes.get();
      i++;
    }
    IOUtils.cleanup(null, reader, channel, fs);
    assertEquals(tupleNum, i);
    assertEquals(status.getLen(), totalRead);
    assertEquals(status.getLen(), reader.readBytes());
  }

  @Test
  public void testLineDelimitedReader() throws IOException {
    TajoConf conf = new TajoConf();
    Path testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    FileSystem fs = testDir.getFileSystem(conf);

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("comment", Type.TEXT);
    schema.addColumn("comment2", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.TEXTFILE);
    meta.putOption("compression.codec", DeflateCodec.class.getCanonicalName());

    Path tablePath = new Path(testDir, "line1." + DeflateCodec.class.getSimpleName());
    FileAppender appender = (FileAppender) StorageManager.getStorageManager(conf).getAppender(meta, schema,
        tablePath);
    appender.enableStats();
    appender.init();
    int tupleNum = 10000;
    VTuple vTuple;

    long splitOffset = 0;
    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(25l));
      vTuple.put(2, DatumFactory.createText("emiya muljomdao"));
      vTuple.put(3, NullDatum.get());
      appender.addTuple(vTuple);

      if(i == (tupleNum / 2)){
        splitOffset = appender.getOffset();
      }
    }
    String extension = ((DelimitedTextFile.DelimitedTextFileAppender) appender).getExtension();
    appender.close();

    tablePath = tablePath.suffix(extension);
    FileFragment fragment = new FileFragment("table", tablePath, 0, splitOffset);
    DelimitedLineReader reader = new DelimitedLineReader(conf, fragment); // if file is compressed, will read to EOF
    assertTrue(reader.isCompressed());
    assertFalse(reader.isReadable());
    reader.init();
    assertTrue(reader.isReadable());


    int i = 0;
    while(reader.isReadable()){
      ByteBuf buf = reader.readLine();
      if(buf == null) break;
      i++;
    }

    IOUtils.cleanup(null, reader, fs);
    assertEquals(tupleNum, i);

  }

  @Test
  public void testByteBufLineReaderWithoutTerminating() throws IOException {
    String path = FileUtil.getResourcePath("dataset/testLineText.txt").getFile();
    File file = new File(path);
    String data = FileUtil.readTextFile(file);

    ByteBufInputChannel channel = new ByteBufInputChannel(new FileInputStream(file));

    assertEquals(file.length(), channel.available());
    ByteBufLineReader reader = new ByteBufLineReader(channel);
    assertEquals(file.length(), reader.available());

    long totalRead = 0;
    int i = 0;
    AtomicInteger bytes = new AtomicInteger();
    for(;;){
      ByteBuf buf = reader.readLineBuf(bytes);
      if(buf == null) break;
      totalRead += bytes.get();
      i++;
    }
    IOUtils.cleanup(null, reader);
    assertEquals(file.length(), totalRead);
    assertEquals(file.length(), reader.readBytes());
    assertEquals(data.split("\n").length, i);
  }
}
