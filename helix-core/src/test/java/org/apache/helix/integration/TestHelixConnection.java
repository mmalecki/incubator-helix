package org.apache.helix.integration;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixParticipant;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.controller.rebalancer.context.SemiAutoRebalancerContext;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.statemachine.HelixStateModelFactory;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixConnection extends ZkUnitTestBase {
  private static final Logger LOG = Logger.getLogger(TestHelixConnection.class.getName());

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "OFFLINE", "ERROR"
  })
  public static class MockStateModel extends StateModel {
    public MockStateModel() {

    }

    @Transition(to = "*", from = "*")
    public void onBecomeAnyFromAny(Message message, NotificationContext context) {
      String from = message.getFromState();
      String to = message.getToState();
      LOG.info("Become " + to + " from " + from);
    }
  }

  public static class MockStateModelFactory extends HelixStateModelFactory<MockStateModel> {

    public MockStateModelFactory() {
    }

    @Override
    public MockStateModel createNewStateModel(PartitionId partitionId) {
      MockStateModel model = new MockStateModel();

      return model;
    }
  }

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    String zkAddr = ZK_ADDR;
    ClusterId clusterId = ClusterId.from(clusterName);
    ControllerId controllerId = ControllerId.from("controller");
    final ParticipantId participantId = ParticipantId.from("participant1");

    ResourceId resourceId = ResourceId.from("testDB");
    State master = State.from("MASTER");
    State slave = State.from("SLAVE");
    State offline = State.from("OFFLINE");
    StateModelDefId stateModelDefId = StateModelDefId.from("MasterSlave");

    // create connection
    HelixConnection connection = new ZkHelixConnection(zkAddr);
    connection.connect();

    // setup cluster
    ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
    clusterAccessor.dropCluster();

    StateModelDefinition stateModelDef =
        new StateModelDefinition.Builder(stateModelDefId).addState(master, 1).addState(slave, 2)
            .addState(offline, 3).addTransition(offline, slave, 3).addTransition(slave, offline, 4)
            .addTransition(slave, master, 2).addTransition(master, slave, 1).initialState(offline)
            .upperBound(master, 1).dynamicUpperBound(slave, "R").build();
    RebalancerContext rebalancerCtx =
        new SemiAutoRebalancerContext.Builder(resourceId).addPartitions(1).replicaCount(1)
            .stateModelDefId(stateModelDefId)
            .preferenceList(PartitionId.from("testDB_0"), Arrays.asList(participantId)).build();
    clusterAccessor.createCluster(new ClusterConfig.Builder(clusterId).addStateModelDefinition(
        stateModelDef).build());
    clusterAccessor.addResourceToCluster(new ResourceConfig.Builder(resourceId).rebalancerContext(
        rebalancerCtx).build());
    clusterAccessor.addParticipantToCluster(new ParticipantConfig.Builder(participantId).build());

    // start controller
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.startAsync();

    // start participant
    HelixParticipant participant = connection.createParticipant(clusterId, participantId);
    participant.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("MasterSlave"), new MockStateModelFactory());

    participant.startAsync();

    // verify
    final HelixDataAccessor accessor = connection.createDataAccessor(clusterId);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    boolean success = TestHelper.verify(new TestHelper.Verifier() {

      @Override
      public boolean verify() throws Exception {
        ExternalView externalView = accessor.getProperty(keyBuilder.externalView("testDB"));
        Map<ParticipantId, State> stateMap = externalView.getStateMap(PartitionId.from("testDB_0"));

        if (stateMap == null || !stateMap.containsKey(participantId)) {
          return false;
        }

        return stateMap.get(participantId).equals(State.from("MASTER"));
      }
    }, 10 * 1000);

    Assert.assertTrue(success);

    // clean up
    controller.stopAsync();
    participant.stopAsync();

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
