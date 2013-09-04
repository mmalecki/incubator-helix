package org.apache.helix.controller.stages;

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
import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Id;
import org.apache.helix.api.MessageId;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.Partition;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.SessionId;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.api.StateModelFactoryId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageState;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

/**
 * Compares the currentState, pendingState with IdealState and generate messages
 */
public class NewMessageGenerationPhase extends AbstractBaseStage {
  private static Logger LOG = Logger.getLogger(NewMessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    Cluster cluster = event.getAttribute("ClusterDataCache");
    Map<ResourceId, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    NewCurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    if (manager == null || cluster == null || resourceMap == null || currentStateOutput == null
        || bestPossibleStateOutput == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager|DataCache|RESOURCES|CURRENT_STATE|BEST_POSSIBLE_STATE");
    }

    // Map<String, LiveInstance> liveInstances = cache.getLiveInstances();
    // Map<String, String> sessionIdMap = new HashMap<String, String>();

    // for (LiveInstance liveInstance : liveInstances.values()) {
    // sessionIdMap.put(liveInstance.getInstanceName(), liveInstance.getSessionId().stringify());
    // }
    MessageGenerationOutput output = new MessageGenerationOutput();

    for (ResourceId resourceId : resourceMap.keySet()) {
      Resource resource = resourceMap.get(resourceId);
      int bucketSize = resource.getRebalancerConfig().getBucketSize();

      // TODO fix it
      StateModelDefinition stateModelDef = null;
      // cache.getStateModelDef(resource.getStateModelDefRef());

      for (PartitionId partitionId : resource.getPartitionMap().keySet()) {
        // TODO fix it
        Map<ParticipantId, State> instanceStateMap = null;
        // bestPossibleStateOutput.getInstanceStateMap(resourceId, partition);

        // we should generate message based on the desired-state priority
        // so keep generated messages in a temp map keyed by state
        // desired-state->list of generated-messages
        Map<State, List<Message>> messageMap = new HashMap<State, List<Message>>();

        for (ParticipantId participantId : instanceStateMap.keySet()) {
          State desiredState = instanceStateMap.get(participantId);

          State currentState =
              currentStateOutput.getCurrentState(resourceId, partitionId, participantId);
          if (currentState == null) {
            // TODO fix it
            // currentState = stateModelDef.getInitialStateString();
          }

          if (desiredState.equals(currentState)) {
            continue;
          }

          State pendingState =
              currentStateOutput.getPendingState(resourceId, partitionId, participantId);

          // TODO fix it
          State nextState = new State("");
          // stateModelDef.getNextStateForTransition(currentState, desiredState);
          if (nextState == null) {
            LOG.error("Unable to find a next state for partition: " + partitionId
                + " from stateModelDefinition"
                + stateModelDef.getClass() + " from:" + currentState + " to:" + desiredState);
            continue;
          }

          if (pendingState != null) {
            if (nextState.equals(pendingState)) {
              LOG.debug("Message already exists for " + participantId + " to transit "
                  + partitionId + " from " + currentState + " to " + nextState);
            } else if (currentState.equals(pendingState)) {
              LOG.info("Message hasn't been removed for " + participantId + " to transit"
                  + partitionId + " to " + pendingState + ", desiredState: "
                  + desiredState);
            } else {
              LOG.info("IdealState changed before state transition completes for " + partitionId
                  + " on " + participantId + ", pendingState: "
                  + pendingState + ", currentState: " + currentState + ", nextState: " + nextState);
            }
          } else {
            // TODO check if instance is alive
            SessionId sessionId =
                cluster.getLiveParticipantMap().get(participantId).getRunningInstance()
                    .getSessionId();
            Message message =
                createMessage(manager, resourceId, partitionId, participantId, currentState,
                    nextState, sessionId, new StateModelDefId(stateModelDef.getId()), resource
                        .getRebalancerConfig()
                        .getStateModelFactoryId(), bucketSize);

            // TODO fix this
            // IdealState idealState = cache.getIdealState(resourceName);
            // if (idealState != null
            // && idealState.getStateModelDefRef().equalsIgnoreCase(
            // DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
            // if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
            // message.getRecord().setMapField(Message.Attributes.INNER_MESSAGE.toString(),
            // idealState.getRecord().getMapField(partition.getPartitionName()));
            // }
            // }
            // Set timeout of needed
            // String stateTransition =
            // currentState + "-" + nextState + "_" + Message.Attributes.TIMEOUT;
            // if (idealState != null) {
            // String timeOutStr = idealState.getRecord().getSimpleField(stateTransition);
            // if (timeOutStr == null
            // && idealState.getStateModelDefRef().equalsIgnoreCase(
            // DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE)) {
            // // scheduled task queue
            // if (idealState.getRecord().getMapField(partition.getPartitionName()) != null) {
            // timeOutStr =
            // idealState.getRecord().getMapField(partition.getPartitionName())
            // .get(Message.Attributes.TIMEOUT.toString());
            // }
            // }
            // if (timeOutStr != null) {
            // try {
            // int timeout = Integer.parseInt(timeOutStr);
            // if (timeout > 0) {
            // message.setExecutionTimeout(timeout);
            // }
            // } catch (Exception e) {
            // logger.error("", e);
            // }
            // }
            // }
            // message.getRecord().setSimpleField("ClusterEventName", event.getName());

            if (!messageMap.containsKey(desiredState)) {
              messageMap.put(desiredState, new ArrayList<Message>());
            }
            messageMap.get(desiredState).add(message);
          }
        }

        // add generated messages to output according to state priority
        List<String> statesPriorityList = stateModelDef.getStatesPriorityStringList();
        for (String state : statesPriorityList) {
          if (messageMap.containsKey(state)) {
            for (Message message : messageMap.get(state)) {
              // TODO fix it
              // output.addMessage(resourceId, partitionId, message);
            }
          }
        }

      } // end of for-each-partition
    }
    event.addAttribute(AttributeName.MESSAGES_ALL.toString(), output);
  }

  private Message createMessage(HelixManager manager, ResourceId resourceId,
      PartitionId partitionId, ParticipantId participantId, State currentState, State nextState,
      SessionId sessionId, StateModelDefId stateModelDefId,
      StateModelFactoryId stateModelFactoryId, int bucketSize) {
  // MessageId uuid = Id.message(UUID.randomUUID().toString());
  // Message message = new Message(MessageType.STATE_TRANSITION, uuid);
  // message.setSrcName(manager.getInstanceName());
  // message.setTgtName(instanceName);
  // message.setMsgState(MessageState.NEW);
  // message.setPartitionId(Id.partition(partitionName));
  // message.setResourceId(Id.resource(resourceName));
  // message.setFromState(State.from(currentState));
  // message.setToState(State.from(nextState));
  // message.setTgtSessionId(Id.session(sessionId));
  // message.setSrcSessionId(Id.session(manager.getSessionId()));
  // message.setStateModelDef(Id.stateModelDef(stateModelDefName));
  // message.setStateModelFactoryName(stateModelFactoryName);
  // message.setBucketSize(bucketSize);
  //
  // return message;
    return null;
  }
}
