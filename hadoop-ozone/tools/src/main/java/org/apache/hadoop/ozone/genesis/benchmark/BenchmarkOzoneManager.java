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

import com.sun.tools.javac.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMBlockProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.genesis.GenesisUtil;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;

@State(Scope.Benchmark)
public class BenchmarkOzoneManager {

  private static final ExcludeList EMPTY_EXCLUDE_LIST = new ExcludeList();

  private static OzoneConfiguration conf;
  private static File storageDir;
  private static OzoneManager om;
  private static SCMStorageConfig scmStorageConfig;
  private static SCMBlockProtocolServer scmBlockServer;
  private static String volumeName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();


  @Setup(Level.Trial)
  public void initialize() throws IOException, AuthenticationException, InterruptedException {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ENABLED, true);
    storageDir = GenesisUtil.getTempPath()
        .resolve(UUID.randomUUID().toString())
        .toFile();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir.toString());

    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "127.0.0.1:0");

    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, 50);


    StorageContainerManager.scmInit(conf);
    scmStorageConfig = new SCMStorageConfig(conf);
    scmBlockServer = new MockSCMBlockProtocolServer();
    scmBlockServer.start();

    final OMStorage omStorage = new OMStorage(conf);
    omStorage.setClusterId(scmStorageConfig.getClusterID());
    omStorage.setScmId(scmStorageConfig.getScmId());
    omStorage.setOmId(UUID.randomUUID().toString());
    omStorage.initialize();


    om = OzoneManager.createOm(null, conf);
    om.start();

    Thread.sleep(1000);

    final String user = UserGroupInformation.getLoginUser().getUserName();

    om.createVolume(new OmVolumeArgs.Builder().setVolume(volumeName)
        .setAdminName(user).setOwnerName(user).build());

    om.createBucket(new OmBucketInfo.Builder().setBucketName(bucketName)
        .setVolumeName(volumeName).build());
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    om.stop();
    scmBlockServer.stop();
    FileUtils.deleteDirectory(storageDir);
  }

  @Benchmark
  public void openKey(Blackhole hole) throws IOException {
    hole.consume(om.openKey(randomOmKeyArgs()));
  }

  @Benchmark
  public void openCommitKey() throws IOException {
    OmKeyArgs keyArgs = randomOmKeyArgs();
    OpenKeySession session = om.openKey(keyArgs);
    keyArgs.setLocationInfoList(Collections.emptyList());
    om.commitKey(keyArgs, session.getId());
  }

  @Benchmark
  public void openAddBlockCommitKey() throws IOException {
    OmKeyArgs keyArgs = randomOmKeyArgs();
    OpenKeySession session = om.openKey(keyArgs);
    OmKeyLocationInfo location = om.allocateBlock(keyArgs, session.getId(), EMPTY_EXCLUDE_LIST);
    keyArgs.setLocationInfoList(List.of(location));
    om.commitKey(keyArgs, session.getId());
  }

  private OmKeyArgs randomOmKeyArgs() {
    return new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(0)
        .setFactor(ReplicationFactor.THREE)
        .setType(ReplicationType.RATIS)
        .build();
  }

  private class MockSCMBlockProtocolServer extends SCMBlockProtocolServer {

    private final Pipeline pipeline;

    private MockSCMBlockProtocolServer() throws IOException {
      super(conf, null);
      this.pipeline = Pipeline.newBuilder()
          .setId(PipelineID.randomId())
          .setType(ReplicationType.RATIS)
          .setState(Pipeline.PipelineState.OPEN)
          .setFactor(ReplicationFactor.THREE)
          .setNodes(Stream.of(1, 2, 3)
              .map(i -> randomDatanodeDetails())
              .collect(Collectors.toList()))
          .build();
    }

    private DatanodeDetails randomDatanodeDetails() {
      return DatanodeDetails.newBuilder()
          .setUuid(UUID.randomUUID().toString())
          .setHostName("localhost")
          .setIpAddress("127.0.0.1")
          .addPort(DatanodeDetails.newPort(
              DatanodeDetails.Port.Name.RATIS, 0))
          .build();
    }

    @Override
    public AllocatedBlock allocateBlock(final long size,
                                        final ReplicationType type,
                                        final ReplicationFactor factor,
                                        final String owner,
                                        final ExcludeList excludeList) {
      return new AllocatedBlock.Builder()
          .setPipeline(pipeline)
          .setContainerBlockID(new ContainerBlockID(1L,
              ThreadLocalRandom.current().nextLong()))
          .build();
    }

    @Override
    public ScmInfo getScmInfo() {
      return new ScmInfo.Builder()
          .setClusterId(scmStorageConfig.getClusterID())
          .setScmId(scmStorageConfig.getScmId())
          .build();
    }


    @Override
    public void stop() {
      getBlockRpcServer().stop();
    }
  }

}
