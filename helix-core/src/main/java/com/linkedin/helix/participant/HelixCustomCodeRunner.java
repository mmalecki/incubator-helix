package com.linkedin.helix.participant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixConstants.StateModelToken;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;

/**
 * This provides the ability for users to run a custom code in exactly one
 * process using a LeaderStandBy state model. <br/>
 * A typical use case is when one uses CUSTOMIZED ideal state mode where the
 * assignment of partition to nodes needs to change dynamically as the nodes go
 * online/offline.<br/>
 * <code>
 * HelixCustomCodeRunner runner = new HelixCustomCodeRunner(manager,ZK_ADDR);
 * runner
 *  .invoke(_callback)
 *  .on(ChangeType.LIVE_INSTANCE, ChangeType.IdealState)
 *  .usingLeaderStandbyModel("someUniqueId")
 *  .run()
 * </code>
 * 
 * @author kgopalak
 * 
 */
public class HelixCustomCodeRunner
{
  private static Logger LOG = Logger.getLogger(HelixCustomCodeRunner.class);
  private static String PARTICIPANT_LEADER = "PARTICIPANT_LEADER";

  private CustomCodeCallbackHandler _callback;
  private List<ChangeType> _notificationTypes;
  private String _resGroupName;
  private final HelixManager _manager;
  private final String _zkAddr;

  /**
   * Constructs a HelixCustomCodeRunner that will run exactly in one place
   * 
   * @param manager
   * @param zkAddr
   */
  public HelixCustomCodeRunner(HelixManager manager, String zkAddr)
  {
    _manager = manager;
    _zkAddr = zkAddr;
  }

  /**
   * callback to invoke when there is a change in cluster state specified by on(
   * notificationTypes) This callback must be idempotent which means they should
   * not depend on what changed instead simply read the cluster data and act on
   * it.
   * 
   * @param callback
   * @return
   */
  public HelixCustomCodeRunner invoke(CustomCodeCallbackHandler callback)
  {
    _callback = callback;
    return this;
  }

  /**
   * ChangeTypes interested in, ParticipantLeaderCallback.callback method will
   * be invoked on the
   * 
   * @param notificationTypes
   * @return
   */
  public HelixCustomCodeRunner on(ChangeType... notificationTypes)
  {
    _notificationTypes = Arrays.asList(notificationTypes);
    return this;
  }

  public HelixCustomCodeRunner usingLeaderStandbyModel(String id)
  {
    _resGroupName = PARTICIPANT_LEADER + "_" + id;
    return this;
  }

  /**
   * This method will be invoked when there is a change in any subscribed
   * notificationTypes
   * 
   * @throws Exception
   */
  public void start() throws Exception
  {
    if (_callback == null || _notificationTypes == null
        || _notificationTypes.size() == 0 || _resGroupName == null)
    {
      throw new IllegalArgumentException(
          "Require callback | notificationTypes | resourceGroupName");
    }

    GenericLeaderStandbyStateModelFactory stateModelFty = new GenericLeaderStandbyStateModelFactory(
        _callback, _notificationTypes);

    StateMachineEngine stateMach = _manager.getStateMachineEngine();
    stateMach.registerStateModelFactory("LeaderStandby", _resGroupName,
        stateModelFty);

    // manually add ideal state for participant leader using LeaderStandby model
    ZkClient zkClient = new ZkClient(_zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    DataAccessor accessor = new ZKDataAccessor(_manager.getClusterName(),
        zkClient);

    IdealState idealState = new IdealState(_resGroupName);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    idealState.setReplicas(StateModelToken.ANY_LIVEINSTANCE.toString());
    idealState.setNumPartitions(1);
    idealState.setStateModelDefRef("LeaderStandby");
    List<String> prefList = new ArrayList<String>(
        Arrays.asList(StateModelToken.ANY_LIVEINSTANCE.toString()));
    idealState.getRecord().setListField(_resGroupName + "_0", prefList);

    List<String> idealStates = accessor.getChildNames(PropertyType.IDEALSTATES);
    while (idealStates == null || !idealStates.contains(_resGroupName))
    {
      accessor.setProperty(PropertyType.IDEALSTATES, idealState, _resGroupName);
      idealStates = accessor.getChildNames(PropertyType.IDEALSTATES);
    }

    LOG.debug("Set idealState for participantLeader:" + _resGroupName
        + ", idealState:" + idealState);
  }

  public void stop()
  {

  }
}