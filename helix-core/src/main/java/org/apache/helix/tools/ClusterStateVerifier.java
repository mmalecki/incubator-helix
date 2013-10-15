package org.apache.helix.tools;

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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.NewBestPossibleStateCalcStage;
import org.apache.helix.controller.stages.NewBestPossibleStateOutput;
import org.apache.helix.controller.stages.NewCurrentStateComputationStage;
import org.apache.helix.controller.stages.NewResourceComputationStage;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.builder.ResourceAssignmentBuilder;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;

public class ClusterStateVerifier {
  public static String cluster = "cluster";
  public static String zkServerAddress = "zkSvr";
  public static String help = "help";
  public static String timeout = "timeout";
  public static String period = "period";

  private static Logger LOG = Logger.getLogger(ClusterStateVerifier.class);

  public interface Verifier {
    boolean verify();
  }

  public interface ZkVerifier extends Verifier {
    ZkClient getZkClient();

    String getClusterName();
  }

  static class ExtViewVeriferZkListener implements IZkChildListener, IZkDataListener {
    final CountDownLatch _countDown;
    final ZkClient _zkClient;
    final Verifier _verifier;

    public ExtViewVeriferZkListener(CountDownLatch countDown, ZkClient zkClient, ZkVerifier verifier) {
      _countDown = countDown;
      _zkClient = zkClient;
      _verifier = verifier;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      boolean result = _verifier.verify();
      if (result == true) {
        _countDown.countDown();
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // TODO Auto-generated method stub

    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      for (String child : currentChilds) {
        String childPath = parentPath.equals("/") ? parentPath + child : parentPath + "/" + child;
        _zkClient.subscribeDataChanges(childPath, this);
      }

      boolean result = _verifier.verify();
      if (result == true) {
        _countDown.countDown();
      }
    }

  }

  /**
   * verifier that verifies best possible state and external view
   */
  public static class BestPossAndExtViewZkVerifier implements ZkVerifier {
    private final String zkAddr;
    private final String clusterName;
    private final Map<String, Map<String, String>> errStates;
    private final ZkClient zkClient;

    public BestPossAndExtViewZkVerifier(String zkAddr, String clusterName) {
      this(zkAddr, clusterName, null);
    }

    public BestPossAndExtViewZkVerifier(String zkAddr, String clusterName,
        Map<String, Map<String, String>> errStates) {
      if (zkAddr == null || clusterName == null) {
        throw new IllegalArgumentException("requires zkAddr|clusterName");
      }
      this.zkAddr = zkAddr;
      this.clusterName = clusterName;
      this.errStates = errStates;
      this.zkClient = ZKClientPool.getZkClient(zkAddr); // null;
    }

    @Override
    public boolean verify() {
      try {
        HelixDataAccessor accessor =
            new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

        return ClusterStateVerifier.verifyBestPossAndExtView(accessor, errStates, clusterName);
      } catch (Exception e) {
        LOG.error("exception in verification", e);
      }
      return false;
    }

    @Override
    public ZkClient getZkClient() {
      return zkClient;
    }

    @Override
    public String getClusterName() {
      return clusterName;
    }

    @Override
    public String toString() {
      String verifierName = getClass().getName();
      verifierName =
          verifierName.substring(verifierName.lastIndexOf('.') + 1, verifierName.length());
      return verifierName + "(" + clusterName + "@" + zkAddr + ")";
    }
  }

  public static class MasterNbInExtViewVerifier implements ZkVerifier {
    private final String zkAddr;
    private final String clusterName;
    private final ZkClient zkClient;

    public MasterNbInExtViewVerifier(String zkAddr, String clusterName) {
      if (zkAddr == null || clusterName == null) {
        throw new IllegalArgumentException("requires zkAddr|clusterName");
      }
      this.zkAddr = zkAddr;
      this.clusterName = clusterName;
      this.zkClient = ZKClientPool.getZkClient(zkAddr);
    }

    @Override
    public boolean verify() {
      try {
        ZKHelixDataAccessor accessor =
            new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));

        return ClusterStateVerifier.verifyMasterNbInExtView(accessor);
      } catch (Exception e) {
        LOG.error("exception in verification", e);
      }
      return false;
    }

    @Override
    public ZkClient getZkClient() {
      return zkClient;
    }

    @Override
    public String getClusterName() {
      return clusterName;
    }

  }

  static boolean verifyBestPossAndExtView(HelixDataAccessor accessor,
      Map<String, Map<String, String>> errStates, String clusterName) {
    try {
      Builder keyBuilder = accessor.keyBuilder();

      Map<String, IdealState> idealStates = accessor.getChildValuesMap(keyBuilder.idealStates());
      if (idealStates == null) {
        // ideal state is null because ideal state is dropped
        idealStates = Collections.emptyMap();
      }

      Map<String, ExternalView> extViews = accessor.getChildValuesMap(keyBuilder.externalViews());
      if (extViews == null) {
        extViews = Collections.emptyMap();
      }

      // if externalView is not empty and idealState doesn't exist
      // add empty idealState for the resource
      for (String resource : extViews.keySet()) {
        if (!idealStates.containsKey(resource)) {
          idealStates.put(resource, new IdealState(resource));
        }
      }

      ClusterAccessor clusterAccessor = new ClusterAccessor(ClusterId.from(clusterName), accessor);
      Cluster cluster = clusterAccessor.readCluster();
      // calculate best possible state
      NewBestPossibleStateOutput bestPossOutput = ClusterStateVerifier.calcBestPossState(cluster);

      // set error states
      if (errStates != null) {
        for (String resourceName : errStates.keySet()) {
          ResourceId resourceId = ResourceId.from(resourceName);
          Map<String, String> partErrStates = errStates.get(resourceName);
          ResourceAssignment resourceAssignment = bestPossOutput.getResourceAssignment(resourceId);

          ResourceAssignmentBuilder raBuilder = new ResourceAssignmentBuilder(resourceId);
          List<? extends PartitionId> mappedPartitions = resourceAssignment.getMappedPartitionIds();
          for (PartitionId partitionId : mappedPartitions) {
            raBuilder.addAssignments(partitionId, resourceAssignment.getReplicaMap(partitionId));
          }

          for (String partitionName : partErrStates.keySet()) {
            String instanceName = partErrStates.get(partitionName);
            PartitionId partitionId = PartitionId.from(partitionName);
            ParticipantId participantId = ParticipantId.from(instanceName);
            raBuilder.addAssignment(partitionId, participantId,
                State.from(HelixDefinedState.ERROR.toString()));
          }
          bestPossOutput.setResourceAssignment(resourceId, raBuilder.build());

        }
      }

      // System.out.println("stateMap: " + bestPossStateMap);

      for (String resourceName : idealStates.keySet()) {
        ExternalView extView = extViews.get(resourceName);
        if (extView == null) {
          LOG.info("externalView for " + resourceName + " is not available");
          return false;
        }

        // step 0: remove empty map and DROPPED state from best possible state
        Map<String, Map<String, String>> bpStateMap =
            ResourceAssignment.stringMapsFromReplicaMaps(bestPossOutput.getResourceAssignment(
                ResourceId.from(resourceName)).getResourceMap());
        Iterator<Entry<String, Map<String, String>>> iter = bpStateMap.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, Map<String, String>> entry = iter.next();
          Map<String, String> instanceStateMap = entry.getValue();
          if (instanceStateMap.isEmpty()) {
            iter.remove();
          } else {
            // remove instances with DROPPED state
            Iterator<Map.Entry<String, String>> insIter = instanceStateMap.entrySet().iterator();
            while (insIter.hasNext()) {
              Map.Entry<String, String> insEntry = insIter.next();
              String state = insEntry.getValue();
              if (state.equalsIgnoreCase(HelixDefinedState.DROPPED.toString())) {
                insIter.remove();
              }
            }
          }
        }

        // System.err.println("resource: " + resourceName + ", bpStateMap: " + bpStateMap);

        // step 1: externalView and bestPossibleState has equal size
        int extViewSize = extView.getRecord().getMapFields().size();
        int bestPossStateSize =
            bestPossOutput.getResourceAssignment(ResourceId.from(resourceName))
                .getMappedPartitionIds().size();
        if (extViewSize != bestPossStateSize) {
          LOG.info("exterView size (" + extViewSize + ") is different from bestPossState size ("
              + bestPossStateSize + ") for resource: " + resourceName);

          // System.err.println("exterView size (" + extViewSize
          // + ") is different from bestPossState size (" + bestPossStateSize
          // + ") for resource: " + resourceName);
          // System.out.println("extView: " + extView.getRecord().getMapFields());
          // System.out.println("bestPossState: " +
          // bestPossOutput.getResourceMap(resourceName));
          return false;
        }

        // step 2: every entry in external view is contained in best possible state
        for (String partition : extView.getRecord().getMapFields().keySet()) {
          Map<String, String> evInstanceStateMap = extView.getRecord().getMapField(partition);
          Map<String, String> bpInstanceStateMap =
              ResourceAssignment.stringMapFromReplicaMap(bestPossOutput.getResourceAssignment(
                  ResourceId.from(resourceName)).getReplicaMap(PartitionId.from(partition)));

          boolean result =
              ClusterStateVerifier.<String, String> compareMap(evInstanceStateMap,
                  bpInstanceStateMap);
          if (result == false) {
            LOG.info("externalView is different from bestPossibleState for partition:" + partition);

            // System.err.println("externalView is different from bestPossibleState for partition: "
            // + partition + ", actual: " + evInstanceStateMap + ", bestPoss: " +
            // bpInstanceStateMap);
            return false;
          }
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error("exception in verification", e);
      return false;
    }

  }

  static boolean verifyMasterNbInExtView(HelixDataAccessor accessor) {
    Builder keyBuilder = accessor.keyBuilder();

    Map<String, IdealState> idealStates = accessor.getChildValuesMap(keyBuilder.idealStates());
    if (idealStates == null || idealStates.size() == 0) {
      LOG.info("No resource idealState");
      return true;
    }

    Map<String, ExternalView> extViews = accessor.getChildValuesMap(keyBuilder.externalViews());
    if (extViews == null || extViews.size() < idealStates.size()) {
      LOG.info("No externalViews | externalView.size() < idealState.size()");
      return false;
    }

    for (String resource : extViews.keySet()) {
      int partitions = idealStates.get(resource).getNumPartitions();
      Map<String, Map<String, String>> instanceStateMap =
          extViews.get(resource).getRecord().getMapFields();
      if (instanceStateMap.size() < partitions) {
        LOG.info("Number of externalViews (" + instanceStateMap.size() + ") < partitions ("
            + partitions + ")");
        return false;
      }

      for (String partition : instanceStateMap.keySet()) {
        boolean foundMaster = false;
        for (String instance : instanceStateMap.get(partition).keySet()) {
          if (instanceStateMap.get(partition).get(instance).equalsIgnoreCase("MASTER")) {
            foundMaster = true;
            break;
          }
        }
        if (!foundMaster) {
          LOG.info("No MASTER for partition: " + partition);
          return false;
        }
      }
    }
    return true;
  }

  static void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  /**
   * calculate the best possible state note that DROPPED states are not checked since when
   * kick off the BestPossibleStateCalcStage we are providing an empty current state map
   * @param convertedDefs
   * @param cache
   * @return
   * @throws Exception
   */

  static NewBestPossibleStateOutput calcBestPossState(Cluster cluster) throws Exception {
    ClusterEvent event = new ClusterEvent("sampleEvent");
    event.addAttribute("ClusterDataCache", cluster);

    NewResourceComputationStage rcState = new NewResourceComputationStage();
    NewCurrentStateComputationStage csStage = new NewCurrentStateComputationStage();
    NewBestPossibleStateCalcStage bpStage = new NewBestPossibleStateCalcStage();

    runStage(event, rcState);
    runStage(event, csStage);
    runStage(event, bpStage);

    NewBestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());

    return output;
  }

  public static <K, V> boolean compareMap(Map<K, V> map1, Map<K, V> map2) {
    boolean isEqual = true;
    if (map1 == null && map2 == null) {
      // OK
    } else if (map1 == null && map2 != null) {
      if (!map2.isEmpty()) {
        isEqual = false;
      }
    } else if (map1 != null && map2 == null) {
      if (!map1.isEmpty()) {
        isEqual = false;
      }
    } else {
      // verify size
      if (map1.size() != map2.size()) {
        isEqual = false;
      }
      // verify each <key, value> in map1 is contained in map2
      for (K key : map1.keySet()) {
        if (!map1.get(key).equals(map2.get(key))) {
          LOG.debug("different value for key: " + key + "(map1: " + map1.get(key) + ", map2: "
              + map2.get(key) + ")");
          isEqual = false;
          break;
        }
      }
    }
    return isEqual;
  }

  public static boolean verifyByPolling(Verifier verifier) {
    return verifyByPolling(verifier, 30 * 1000);
  }

  public static boolean verifyByPolling(Verifier verifier, long timeout) {
    return verifyByPolling(verifier, timeout, 1000);
  }

  public static boolean verifyByPolling(Verifier verifier, long timeout, long period) {
    long startTime = System.currentTimeMillis();
    boolean result = false;
    try {
      long curTime;
      do {
        Thread.sleep(period);
        result = verifier.verify();
        if (result == true) {
          break;
        }
        curTime = System.currentTimeMillis();
      } while (curTime <= startTime + timeout);
      return result;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      long endTime = System.currentTimeMillis();

      // debug
      System.err.println(result + ": " + verifier + ": wait " + (endTime - startTime)
          + "ms to verify");

    }
    return false;
  }

  public static boolean verifyByZkCallback(ZkVerifier verifier) {
    return verifyByZkCallback(verifier, 30000);
  }

  public static boolean verifyByZkCallback(ZkVerifier verifier, long timeout) {
    long startTime = System.currentTimeMillis();
    CountDownLatch countDown = new CountDownLatch(1);
    ZkClient zkClient = verifier.getZkClient();
    String clusterName = verifier.getClusterName();

    // add an ephemeral node to /{clusterName}/CONFIGS/CLUSTER/verify
    // so when analyze zk log, we know when a test ends
    zkClient.createEphemeral("/" + clusterName + "/CONFIGS/CLUSTER/verify");

    ExtViewVeriferZkListener listener = new ExtViewVeriferZkListener(countDown, zkClient, verifier);

    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName);
    zkClient.subscribeChildChanges(extViewPath, listener);
    for (String child : zkClient.getChildren(extViewPath)) {
      String childPath = extViewPath.equals("/") ? extViewPath + child : extViewPath + "/" + child;
      zkClient.subscribeDataChanges(childPath, listener);
    }

    // do initial verify
    boolean result = verifier.verify();
    if (result == false) {
      try {
        result = countDown.await(timeout, TimeUnit.MILLISECONDS);
        if (result == false) {
          // make a final try if timeout
          result = verifier.verify();
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // clean up
    zkClient.unsubscribeChildChanges(extViewPath, listener);
    for (String child : zkClient.getChildren(extViewPath)) {
      String childPath = extViewPath.equals("/") ? extViewPath + child : extViewPath + "/" + child;
      zkClient.unsubscribeDataChanges(childPath, listener);
    }

    long endTime = System.currentTimeMillis();

    zkClient.delete("/" + clusterName + "/CONFIGS/CLUSTER/verify");
    // debug
    System.err.println(result + ": wait " + (endTime - startTime) + "ms, " + verifier);

    return result;
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServerAddress).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option timeoutOption =
        OptionBuilder.withLongOpt(timeout).withDescription("Timeout value for verification")
            .create();
    timeoutOption.setArgs(1);
    timeoutOption.setArgName("Timeout value (Optional), default=30s");

    Option sleepIntervalOption =
        OptionBuilder.withLongOpt(period).withDescription("Polling period for verification")
            .create();
    sleepIntervalOption.setArgs(1);
    sleepIntervalOption.setArgName("Polling period value (Optional), default=1s");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(timeoutOption);
    options.addOption(sleepIntervalOption);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs) {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    // CommandLine cmd = null;

    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static boolean verifyState(String[] args) {
    // TODO Auto-generated method stub
    String clusterName = "storage-cluster";
    String zkServer = "localhost:2181";
    long timeoutValue = 0;
    long periodValue = 1000;

    if (args.length > 0) {
      CommandLine cmd = processCommandLineArgs(args);
      zkServer = cmd.getOptionValue(zkServerAddress);
      clusterName = cmd.getOptionValue(cluster);
      String timeoutStr = cmd.getOptionValue(timeout);
      String periodStr = cmd.getOptionValue(period);
      if (timeoutStr != null) {
        try {
          timeoutValue = Long.parseLong(timeoutStr);
        } catch (Exception e) {
          System.err.println("Exception in converting " + timeoutStr + " to long. Use default (0)");
        }
      }

      if (periodStr != null) {
        try {
          periodValue = Long.parseLong(periodStr);
        } catch (Exception e) {
          System.err.println("Exception in converting " + periodStr
              + " to long. Use default (1000)");
        }
      }

    }
    // return verifyByPolling(new BestPossAndExtViewZkVerifier(zkServer, clusterName),
    // timeoutValue,
    // periodValue);

    return verifyByZkCallback(new BestPossAndExtViewZkVerifier(zkServer, clusterName), timeoutValue);
  }

  public static void main(String[] args) {
    boolean result = verifyState(args);
    System.out.println(result ? "Successful" : "failed");
    System.exit(1);
  }

}
