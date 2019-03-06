/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.genesis.benchmark;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.genesis.GenesisUtil;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.RocksDBStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.rocksdb.Options;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@State(Scope.Benchmark)
public class BenchmarkRocksDB {

  private static final int DATA_LEN = 1024;
  private static final Charset CHARSET_UTF_8 = Charset.forName("UTF-8");

  private static File storageDir;
  private static MetadataStore store;
  private static byte[] data;


  @Setup(Level.Trial)
  public void initialize() throws IOException {
    data = RandomStringUtils.randomAlphanumeric(DATA_LEN)
        .getBytes(CHARSET_UTF_8);

    storageDir = GenesisUtil.getTempPath()
        .resolve(UUID.randomUUID().toString())
        .toFile();

    Options options = new Options();
    options.setUseFsync(false);
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);
    options.setMaxBackgroundCompactions(4);
    options.setBytesPerSync(8388608);

    store = new RocksDBStore(storageDir, options);
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    store.destroy();
    FileUtils.deleteDirectory(storageDir);
  }

  @Benchmark
  public void write() throws IOException {
    int i = ThreadLocalRandom.current().nextInt();
    store.put(Integer.toBinaryString(i).getBytes(CHARSET_UTF_8), data);
  }

}
