package org.apache.helix.api.accessor;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.RunningInstance;
import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.PartitionedRebalancerContext;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ParticipantAccessor {
  private static final Logger LOG = Logger.getLogger(ParticipantAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  public ParticipantAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * enable/disable a participant
   * @param participantId
   * @param isEnabled
   */
  void enableParticipant(ParticipantId participantId, boolean isEnabled) {
    String participantName = participantId.stringify();
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantName)) == null) {
      LOG.error("Config for participant: " + participantId + " does NOT exist in cluster");
      return;
    }

    InstanceConfig config = new InstanceConfig(participantName);
    config.setInstanceEnabled(isEnabled);
    _accessor.updateProperty(_keyBuilder.instanceConfig(participantName), config);

  }

  /**
   * disable participant
   * @param participantId
   */
  public void disableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, false);
  }

  /**
   * enable participant
   * @param participantId
   */
  public void enableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, true);
  }

  /**
   * create messages for participant
   * @param participantId
   * @param msgMap map of message-id to message
   */
  public void insertMessagesToParticipant(ParticipantId participantId,
      Map<MessageId, Message> msgMap) {
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantId.stringify(), msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }

    _accessor.createChildren(msgKeys, msgs);
  }

  /**
   * set messages of participant
   * @param participantId
   * @param msgMap map of message-id to message
   */
  public void updateMessageStatus(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    String participantName = participantId.stringify();
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantName, msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }
    _accessor.setChildren(msgKeys, msgs);
  }

  /**
   * delete messages from participant
   * @param participantId
   * @param msgIdSet
   */
  public void deleteMessagesFromParticipant(ParticipantId participantId, Set<MessageId> msgIdSet) {
    String participantName = participantId.stringify();
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    for (MessageId msgId : msgIdSet) {
      msgKeys.add(_keyBuilder.message(participantName, msgId.stringify()));
    }

    // TODO impl batch remove
    for (PropertyKey msgKey : msgKeys) {
      _accessor.removeProperty(msgKey);
    }
  }

  /**
   * enable/disable partitions on a participant
   * @param enabled
   * @param participantId
   * @param resourceId
   * @param partitionIdSet
   */
  void enablePartitionsForParticipant(final boolean enabled, final ParticipantId participantId,
      final ResourceId resourceId, final Set<PartitionId> partitionIdSet) {
    String participantName = participantId.stringify();
    String resourceName = resourceId.stringify();

    // check instanceConfig exists
    PropertyKey instanceConfigKey = _keyBuilder.instanceConfig(participantName);
    if (_accessor.getProperty(instanceConfigKey) == null) {
      LOG.error("Config for participant: " + participantId + " does NOT exist in cluster");
      return;
    }

    // check resource exist. warn if not
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealState(resourceName));
    if (idealState == null) {
      LOG.warn("Disable partitions: " + partitionIdSet + ", resource: " + resourceId
          + " does NOT exist. probably disable it during ERROR->DROPPED transtition");

    } else {
      // check partitions exist. warn if not
      for (PartitionId partitionId : partitionIdSet) {
        if ((idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO && idealState
            .getPreferenceList(partitionId) == null)
            || (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED && idealState
                .getParticipantStateMap(partitionId) == null)) {
          LOG.warn("Resource: " + resourceId + ", partition: " + partitionId
              + ", partition does NOT exist in ideal state");
        }
      }
    }

    // TODO merge list logic should go to znrecord updater
    // update participantConfig
    // could not use ZNRecordUpdater since it doesn't do listField merge/subtract
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    final List<String> partitionNames = new ArrayList<String>();
    for (PartitionId partitionId : partitionIdSet) {
      partitionNames.add(partitionId.stringify());
    }

    baseAccessor.update(instanceConfigKey.getPath(), new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Instance: " + participantId + ", participant config is null");
        }

        // TODO: merge with InstanceConfig.setInstanceEnabledForPartition
        List<String> list =
            currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
        Set<String> disabledPartitions = new HashSet<String>();
        if (list != null) {
          disabledPartitions.addAll(list);
        }

        if (enabled) {
          disabledPartitions.removeAll(partitionNames);
        } else {
          disabledPartitions.addAll(partitionNames);
        }

        list = new ArrayList<String>(disabledPartitions);
        Collections.sort(list);
        currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list);
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * disable partitions on a participant
   * @param participantId
   * @param resourceId
   * @param disablePartitionIdSet
   */
  public void disablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> disablePartitionIdSet) {
    enablePartitionsForParticipant(false, participantId, resourceId, disablePartitionIdSet);
  }

  /**
   * enable partitions on a participant
   * @param participantId
   * @param resourceId
   * @param enablePartitionIdSet
   */
  public void enablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> enablePartitionIdSet) {
    enablePartitionsForParticipant(true, participantId, resourceId, enablePartitionIdSet);
  }

  /**
   * Reset partitions assigned to a set of participants
   * @param resetParticipantIdSet the participants to reset
   * @return true if reset, false otherwise
   */
  public boolean resetParticipants(Set<ParticipantId> resetParticipantIdSet) {
    List<ExternalView> extViews = _accessor.getChildValues(_keyBuilder.externalViews());
    for (ParticipantId participantId : resetParticipantIdSet) {
      for (ExternalView extView : extViews) {
        Set<PartitionId> resetPartitionIdSet = Sets.newHashSet();
        for (PartitionId partitionId : extView.getPartitionSet()) {
          Map<ParticipantId, State> stateMap = extView.getStateMap(partitionId);
          if (stateMap.containsKey(participantId)
              && stateMap.get(participantId).equals(State.from(HelixDefinedState.ERROR))) {
            resetPartitionIdSet.add(partitionId);
          }
        }
        resetPartitionsForParticipant(participantId, extView.getResourceId(), resetPartitionIdSet);
      }
    }
    return true;
  }

  /**
   * reset partitions on a participant
   * @param participantId
   * @param resourceId
   * @param resetPartitionIdSet
   * @return true if partitions reset, false otherwise
   */
  public boolean resetPartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> resetPartitionIdSet) {
    // make sure the participant is running
    Participant participant = readParticipant(participantId);
    if (!participant.isAlive()) {
      LOG.error("Cannot reset partitions because the participant is not running");
      return false;
    }
    RunningInstance runningInstance = participant.getRunningInstance();

    // check that the resource exists
    ResourceAccessor resourceAccessor = new ResourceAccessor(_accessor);
    Resource resource = resourceAccessor.readResource(resourceId);
    if (resource == null || resource.getRebalancerConfig() == null) {
      LOG.error("Cannot reset partitions because the resource is not present");
      return false;
    }

    // need the rebalancer context for the resource
    RebalancerContext context =
        resource.getRebalancerConfig().getRebalancerContext(RebalancerContext.class);
    if (context == null) {
      LOG.error("Rebalancer context for resource does not exist");
      return false;
    }

    // ensure that all partitions to reset exist
    Set<PartitionId> partitionSet = ImmutableSet.copyOf(context.getSubUnitIdSet());
    if (!partitionSet.containsAll(resetPartitionIdSet)) {
      LOG.error("Not all of the specified partitions to reset exist for the resource");
      return false;
    }

    // check for a valid current state that has all specified partitions in ERROR state
    CurrentState currentState = participant.getCurrentStateMap().get(resourceId);
    if (currentState == null) {
      LOG.error("The participant does not have a current state for the resource");
      return false;
    }
    for (PartitionId partitionId : resetPartitionIdSet) {
      if (!currentState.getState(partitionId).equals(State.from(HelixDefinedState.ERROR))) {
        LOG.error("Partition " + partitionId + " is not in error state, aborting reset");
        return false;
      }
    }

    // make sure that there are no pending transition messages
    for (Message message : participant.getMessageMap().values()) {
      if (!MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType())
          || !runningInstance.getSessionId().equals(message.getTgtSessionId())
          || !resourceId.equals(message.getResourceId())
          || !resetPartitionIdSet.contains(message.getPartitionId())) {
        continue;
      }
      LOG.error("Cannot reset partitions because of the following pending message: " + message);
      return false;
    }

    // set up the source id
    String adminName = null;
    try {
      adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN";
    } catch (UnknownHostException e) {
      // can ignore it
      if (LOG.isInfoEnabled()) {
        LOG.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e);
      }
      adminName = "UNKNOWN";
    }

    // build messages to signal the transition
    StateModelDefId stateModelDefId = context.getStateModelDefId();
    StateModelDefinition stateModelDef =
        _accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify()));
    Map<MessageId, Message> messageMap = Maps.newHashMap();
    for (PartitionId partitionId : resetPartitionIdSet) {
      // send ERROR to initialState message
      MessageId msgId = MessageId.from(UUID.randomUUID().toString());
      Message message = new Message(MessageType.STATE_TRANSITION, msgId);
      message.setSrcName(adminName);
      message.setTgtName(participantId.stringify());
      message.setMsgState(MessageState.NEW);
      message.setPartitionId(partitionId);
      message.setResourceId(resourceId);
      message.setTgtSessionId(runningInstance.getSessionId());
      message.setStateModelDef(stateModelDefId);
      message.setFromState(State.from(HelixDefinedState.ERROR.toString()));
      message.setToState(stateModelDef.getInitialState());
      message.setStateModelFactoryId(context.getStateModelFactoryId());

      messageMap.put(message.getMsgId(), message);
    }

    // send the messages
    insertMessagesToParticipant(participantId, messageMap);
    return true;
  }

  /**
   * Read the user config of the participant
   * @param participantId the participant to to look up
   * @return UserConfig, or null
   */
  public UserConfig readUserConfig(ParticipantId participantId) {
    InstanceConfig instanceConfig =
        _accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify()));
    return instanceConfig != null ? UserConfig.from(instanceConfig) : null;
  }

  /**
   * Set the user config of the participant, overwriting existing user configs
   * @param participantId the participant to update
   * @param userConfig the new user config
   * @return true if the user config was set, false otherwise
   */
  public boolean setUserConfig(ParticipantId participantId, UserConfig userConfig) {
    ParticipantConfig.Delta delta =
        new ParticipantConfig.Delta(participantId).setUserConfig(userConfig);
    return updateParticipant(participantId, delta) != null;
  }

  /**
   * Add user configuration to the existing participant user configuration. Overwrites properties
   * with
   * the same key
   * @param participant the participant to update
   * @param userConfig the user config key-value pairs to add
   * @return true if the user config was updated, false otherwise
   */
  public boolean updateUserConfig(ParticipantId participantId, UserConfig userConfig) {
    InstanceConfig instanceConfig = new InstanceConfig(participantId);
    instanceConfig.addNamespacedConfig(userConfig);
    return _accessor.updateProperty(_keyBuilder.instanceConfig(participantId.stringify()),
        instanceConfig);
  }

  /**
   * Clear any user-specified configuration from the participant
   * @param participantId the participant to update
   * @return true if the config was cleared, false otherwise
   */
  public boolean dropUserConfig(ParticipantId participantId) {
    return setUserConfig(participantId, new UserConfig(Scope.participant(participantId)));
  }

  /**
   * Update a participant configuration
   * @param participantId the participant to update
   * @param participantDelta changes to the participant
   * @return ParticipantConfig, or null if participant is not persisted
   */
  public ParticipantConfig updateParticipant(ParticipantId participantId,
      ParticipantConfig.Delta participantDelta) {
    Participant participant = readParticipant(participantId);
    if (participant == null) {
      LOG.error("Participant " + participantId + " does not exist, cannot be updated");
      return null;
    }
    ParticipantConfig config = participantDelta.mergeInto(participant.getConfig());
    setParticipant(config);
    return config;
  }

  /**
   * Set the configuration of an existing participant
   * @param participantConfig participant configuration
   * @return true if config was set, false if there was an error
   */
  public boolean setParticipant(ParticipantConfig participantConfig) {
    if (participantConfig == null) {
      LOG.error("Participant config not initialized");
      return false;
    }
    InstanceConfig instanceConfig = new InstanceConfig(participantConfig.getId());
    instanceConfig.setHostName(participantConfig.getHostName());
    instanceConfig.setPort(Integer.toString(participantConfig.getPort()));
    for (String tag : participantConfig.getTags()) {
      instanceConfig.addTag(tag);
    }
    for (PartitionId partitionId : participantConfig.getDisabledPartitions()) {
      instanceConfig.setInstanceEnabledForPartition(partitionId, false);
    }
    instanceConfig.setInstanceEnabled(participantConfig.isEnabled());
    instanceConfig.addNamespacedConfig(participantConfig.getUserConfig());
    _accessor.setProperty(_keyBuilder.instanceConfig(participantConfig.getId().stringify()),
        instanceConfig);
    return true;
  }

  /**
   * create a participant based on physical model
   * @param participantId
   * @param instanceConfig
   * @param userConfig
   * @param liveInstance
   * @param instanceMsgMap map of message-id to message
   * @param instanceCurStateMap map of resource-id to current-state
   * @return participant
   */
  static Participant createParticipant(ParticipantId participantId, InstanceConfig instanceConfig,
      UserConfig userConfig, LiveInstance liveInstance, Map<String, Message> instanceMsgMap,
      Map<String, CurrentState> instanceCurStateMap) {

    String hostName = instanceConfig.getHostName();

    int port = -1;
    try {
      port = Integer.parseInt(instanceConfig.getPort());
    } catch (IllegalArgumentException e) {
      // keep as -1
    }
    if (port < 0 || port > 65535) {
      port = -1;
    }
    boolean isEnabled = instanceConfig.getInstanceEnabled();

    List<String> disabledPartitions = instanceConfig.getDisabledPartitions();
    Set<PartitionId> disabledPartitionIdSet = Collections.emptySet();
    if (disabledPartitions != null) {
      disabledPartitionIdSet = new HashSet<PartitionId>();
      for (String partitionId : disabledPartitions) {
        disabledPartitionIdSet.add(PartitionId.from(PartitionId.extractResourceId(partitionId),
            PartitionId.stripResourceId(partitionId)));
      }
    }

    Set<String> tags = new HashSet<String>(instanceConfig.getTags());

    RunningInstance runningInstance = null;
    if (liveInstance != null) {
      runningInstance =
          new RunningInstance(liveInstance.getSessionId(), liveInstance.getHelixVersion(),
              liveInstance.getProcessId());
    }

    Map<MessageId, Message> msgMap = new HashMap<MessageId, Message>();
    if (instanceMsgMap != null) {
      for (String msgId : instanceMsgMap.keySet()) {
        Message message = instanceMsgMap.get(msgId);
        msgMap.put(MessageId.from(msgId), message);
      }
    }

    Map<ResourceId, CurrentState> curStateMap = new HashMap<ResourceId, CurrentState>();
    if (instanceCurStateMap != null) {

      for (String resourceName : instanceCurStateMap.keySet()) {
        curStateMap.put(ResourceId.from(resourceName), instanceCurStateMap.get(resourceName));
      }
    }

    return new Participant(participantId, hostName, port, isEnabled, disabledPartitionIdSet, tags,
        runningInstance, curStateMap, msgMap, userConfig);
  }

  /**
   * read participant related data
   * @param participantId
   * @return participant, or null if participant not available
   */
  public Participant readParticipant(ParticipantId participantId) {
    // read physical model
    String participantName = participantId.stringify();
    InstanceConfig instanceConfig =
        _accessor.getProperty(_keyBuilder.instanceConfig(participantName));

    if (instanceConfig == null) {
      LOG.error("Participant " + participantId + " is not present on the cluster");
      return null;
    }

    UserConfig userConfig = UserConfig.from(instanceConfig);
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(participantName));

    Map<String, Message> instanceMsgMap = Collections.emptyMap();
    Map<String, CurrentState> instanceCurStateMap = Collections.emptyMap();
    if (liveInstance != null) {
      SessionId sessionId = liveInstance.getSessionId();

      instanceMsgMap = _accessor.getChildValuesMap(_keyBuilder.messages(participantName));
      instanceCurStateMap =
          _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
              sessionId.stringify()));
    }

    return createParticipant(participantId, instanceConfig, userConfig, liveInstance,
        instanceMsgMap, instanceCurStateMap);
  }

  /**
   * update resource current state of a participant
   * @param resourceId resource id
   * @param participantId participant id
   * @param sessionId session id
   * @param curStateUpdate current state change delta
   */
  public void updateCurrentState(ResourceId resourceId, ParticipantId participantId,
      SessionId sessionId, CurrentState curStateUpdate) {
    _accessor.updateProperty(
        _keyBuilder.currentState(participantId.stringify(), sessionId.stringify(),
            resourceId.stringify()), curStateUpdate);
  }

  /**
   * drop resource current state of a participant
   * @param resourceId resource id
   * @param participantId participant id
   * @param sessionId session id
   */
  public void dropCurrentState(ResourceId resourceId, ParticipantId participantId,
      SessionId sessionId) {
    _accessor.removeProperty(_keyBuilder.currentState(participantId.stringify(),
        sessionId.stringify(), resourceId.stringify()));
  }

  /**
   * drop a participant from cluster
   * @param participantId
   * @return true if participant dropped, false if there was an error
   */
  boolean dropParticipant(ParticipantId participantId) {
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) == null) {
      LOG.error("Config for participant: " + participantId + " does NOT exist in cluster");
    }

    if (_accessor.getProperty(_keyBuilder.instance(participantId.stringify())) == null) {
      LOG.error("Participant: " + participantId + " structure does NOT exist in cluster");
    }

    // delete participant config path
    _accessor.removeProperty(_keyBuilder.instanceConfig(participantId.stringify()));

    // delete participant path
    _accessor.removeProperty(_keyBuilder.instance(participantId.stringify()));
    return true;
  }

  /**
   * Let a new participant take the place of an existing participant
   * @param oldParticipantId the participant to drop
   * @param newParticipantId the participant that takes its place
   * @return true if swap successful, false otherwise
   */
  public boolean swapParticipants(ParticipantId oldParticipantId, ParticipantId newParticipantId) {
    Participant oldParticipant = readParticipant(oldParticipantId);
    if (oldParticipant == null) {
      LOG.error("Could not swap participants because the old participant does not exist");
      return false;
    }
    if (oldParticipant.isEnabled()) {
      LOG.error("Could not swap participants because the old participant is still enabled");
      return false;
    }
    if (oldParticipant.isAlive()) {
      LOG.error("Could not swap participants because the old participant is still live");
      return false;
    }
    Participant newParticipant = readParticipant(newParticipantId);
    if (newParticipant == null) {
      LOG.error("Could not swap participants because the new participant does not exist");
      return false;
    }
    dropParticipant(oldParticipantId);
    ResourceAccessor resourceAccessor = new ResourceAccessor(_accessor);
    Map<String, IdealState> idealStateMap = _accessor.getChildValuesMap(_keyBuilder.idealStates());
    for (String resourceName : idealStateMap.keySet()) {
      IdealState idealState = idealStateMap.get(resourceName);
      swapParticipantsInIdealState(idealState, oldParticipantId, newParticipantId);
      PartitionedRebalancerContext context = PartitionedRebalancerContext.from(idealState);
      resourceAccessor.setRebalancerContext(ResourceId.from(resourceName), context);
      _accessor.setProperty(_keyBuilder.idealState(resourceName), idealState);
    }
    return true;
  }

  /**
   * Replace occurrences of participants in preference lists and maps
   * @param idealState the current ideal state
   * @param oldParticipantId the participant to drop
   * @param newParticipantId the participant that replaces it
   */
  private void swapParticipantsInIdealState(IdealState idealState, ParticipantId oldParticipantId,
      ParticipantId newParticipantId) {
    for (PartitionId partitionId : idealState.getPartitionSet()) {
      List<ParticipantId> oldPreferenceList = idealState.getPreferenceList(partitionId);
      if (oldPreferenceList != null) {
        List<ParticipantId> newPreferenceList = Lists.newArrayList();
        for (ParticipantId participantId : oldPreferenceList) {
          if (participantId.equals(oldParticipantId)) {
            newPreferenceList.add(newParticipantId);
          } else if (!participantId.equals(newParticipantId)) {
            newPreferenceList.add(participantId);
          }
        }
        idealState.setPreferenceList(partitionId, newPreferenceList);
      }
      Map<ParticipantId, State> preferenceMap = idealState.getParticipantStateMap(partitionId);
      if (preferenceMap != null) {
        if (preferenceMap.containsKey(oldParticipantId)) {
          State state = preferenceMap.get(oldParticipantId);
          preferenceMap.remove(oldParticipantId);
          preferenceMap.put(newParticipantId, state);
        }
        idealState.setParticipantStateMap(partitionId, preferenceMap);
      }
    }
  }
}
