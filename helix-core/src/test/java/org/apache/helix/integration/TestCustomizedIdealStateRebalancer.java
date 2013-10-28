package org.apache.helix.integration;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.context.PartitionedRebalancerContext;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedIdealStateRebalancer extends
    ZkStandAloneCMTestBaseWithPropertyServerCheck {
  String db2 = TEST_DB + "2";
  static boolean testRebalancerCreated = false;
  static boolean testRebalancerInvoked = false;

  public static class TestRebalancer implements HelixRebalancer {

    /**
     * Very basic mapping that evenly assigns one replica of each partition to live nodes, each of
     * which is in the highest-priority state.
     */
    @Override
    public ResourceAssignment computeResourceMapping(RebalancerConfig config, Cluster cluster,
        ResourceCurrentState currentState) {
      PartitionedRebalancerContext context =
          config.getRebalancerContext(PartitionedRebalancerContext.class);
      StateModelDefinition stateModelDef =
          cluster.getStateModelMap().get(context.getStateModelDefId());
      List<ParticipantId> liveParticipants =
          new ArrayList<ParticipantId>(cluster.getLiveParticipantMap().keySet());
      ResourceAssignment resourceMapping = new ResourceAssignment(context.getResourceId());
      int i = 0;
      for (PartitionId partitionId : context.getPartitionSet()) {
        int nodeIndex = i % liveParticipants.size();
        Map<ParticipantId, State> replicaMap = new HashMap<ParticipantId, State>();
        replicaMap.put(liveParticipants.get(nodeIndex), stateModelDef.getTypedStatesPriorityList()
            .get(0));
        resourceMapping.addReplicaMap(partitionId, replicaMap);
        i++;
      }
      testRebalancerInvoked = true;
      return resourceMapping;
    }

    @Override
    public void init(HelixManager helixManager) {
      testRebalancerCreated = true;
    }
  }

  @Test
  public void testCustomizedIdealStateRebalancer() throws InterruptedException {
    _setupTool.addResourceToCluster(CLUSTER_NAME, db2, 60, "MasterSlave");
    _setupTool.addResourceProperty(CLUSTER_NAME, db2,
        IdealStateProperty.REBALANCER_CLASS_NAME.toString(),
        TestCustomizedIdealStateRebalancer.TestRebalancer.class.getName());
    _setupTool.addResourceProperty(CLUSTER_NAME, db2, IdealStateProperty.REBALANCE_MODE.toString(),
        RebalanceMode.USER_DEFINED.toString());

    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, 3);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ExternalViewBalancedVerifier(_zkClient,
            CLUSTER_NAME, db2));
    Assert.assertTrue(result);
    Thread.sleep(1000);
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(keyBuilder.externalView(db2));
    Assert.assertEquals(ev.getPartitionSet().size(), 60);
    for (String partition : ev.getPartitionSet()) {
      Assert.assertEquals(ev.getStateMap(partition).size(), 1);
    }
    IdealState is = accessor.getProperty(keyBuilder.idealState(db2));
    for (PartitionId partition : is.getPartitionIdSet()) {
      Assert.assertEquals(is.getPreferenceList(partition).size(), 0);
      Assert.assertEquals(is.getParticipantStateMap(partition).size(), 0);
    }
    Assert.assertTrue(testRebalancerCreated);
    Assert.assertTrue(testRebalancerInvoked);
  }

  public static class ExternalViewBalancedVerifier implements ZkVerifier {
    ZkClient _client;
    String _clusterName;
    String _resourceName;

    public ExternalViewBalancedVerifier(ZkClient client, String clusterName, String resourceName) {
      _client = client;
      _clusterName = clusterName;
      _resourceName = resourceName;
    }

    @Override
    public boolean verify() {
      try {
        HelixDataAccessor accessor =
            new ZKHelixDataAccessor(_clusterName, new ZkBaseDataAccessor<ZNRecord>(_client));
        Builder keyBuilder = accessor.keyBuilder();
        IdealState idealState = accessor.getProperty(keyBuilder.idealState(_resourceName));
        int numberOfPartitions = idealState.getRecord().getListFields().size();
        String stateModelDefName = idealState.getStateModelDefId().stringify();
        StateModelDefinition stateModelDef =
            accessor.getProperty(keyBuilder.stateModelDef(stateModelDefName));
        State masterValue = stateModelDef.getTypedStatesPriorityList().get(0);
        int replicas = Integer.parseInt(idealState.getReplicas());
        String instanceGroupTag = idealState.getInstanceGroupTag();
        int instances = 0;
        Map<String, LiveInstance> liveInstanceMap =
            accessor.getChildValuesMap(keyBuilder.liveInstances());
        Map<String, InstanceConfig> instanceCfgMap =
            accessor.getChildValuesMap(keyBuilder.instanceConfigs());
        for (String liveInstanceName : liveInstanceMap.keySet()) {
          if (instanceCfgMap.get(liveInstanceName).containsTag(instanceGroupTag)) {
            instances++;
          }
        }
        if (instances == 0) {
          instances = liveInstanceMap.size();
        }
        ExternalView externalView = accessor.getProperty(keyBuilder.externalView(_resourceName));
        return verifyBalanceExternalView(externalView.getRecord(), numberOfPartitions,
            masterValue.toString(), replicas, instances);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public ZkClient getZkClient() {
      return _client;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }

  }

  static boolean verifyBalanceExternalView(ZNRecord externalView, int partitionCount,
      String masterState, int replica, int instances) {
    Map<String, Integer> masterPartitionsCountMap = new HashMap<String, Integer>();
    for (String partitionName : externalView.getMapFields().keySet()) {
      Map<String, String> assignmentMap = externalView.getMapField(partitionName);
      // Assert.assertTrue(assignmentMap.size() >= replica);
      for (String instance : assignmentMap.keySet()) {
        if (assignmentMap.get(instance).equals(masterState)) {
          if (!masterPartitionsCountMap.containsKey(instance)) {
            masterPartitionsCountMap.put(instance, 0);
          }
          masterPartitionsCountMap.put(instance, masterPartitionsCountMap.get(instance) + 1);
        }
      }
    }

    int perInstancePartition = partitionCount / instances;

    int totalCount = 0;
    for (String instanceName : masterPartitionsCountMap.keySet()) {
      int instancePartitionCount = masterPartitionsCountMap.get(instanceName);
      totalCount += instancePartitionCount;
      if (!(instancePartitionCount == perInstancePartition || instancePartitionCount == perInstancePartition + 1)) {
        return false;
      }
      if (instancePartitionCount == perInstancePartition + 1) {
        if (partitionCount % instances == 0) {
          return false;
        }
      }
    }
    if (partitionCount != totalCount) {
      return false;
    }
    return true;

  }
}
