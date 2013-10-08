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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.context.PartitionedRebalancerContext;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.HelixLockable;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

/**
 * An atomic version of the ParticipantAccessor. If atomic operations are required, use instances of
 * this class. Atomicity is not guaranteed when using instances of ParticipantAccessor alongside
 * instances of this class. Furthermore, depending on the semantics of the lock, lock acquisition
 * may fail, in which case users should handle the return value of each function if necessary.
 */
public class AtomicParticipantAccessor extends ParticipantAccessor {
  private static final Logger LOG = Logger.getLogger(AtomicParticipantAccessor.class);

  private final ClusterId _clusterId;
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final HelixLockable _lockProvider;

  public AtomicParticipantAccessor(ClusterId clusterId, HelixDataAccessor accessor,
      HelixLockable lockProvider) {
    super(accessor);
    _clusterId = clusterId;
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
    _lockProvider = lockProvider;
  }

  @Override
  public Participant readParticipant(ParticipantId participantId) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.readParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public boolean setParticipant(ParticipantConfig participantConfig) {
    if (participantConfig == null) {
      LOG.error("participant config cannot be null");
      return false;
    }
    HelixLock lock =
        _lockProvider.getLock(_clusterId, Scope.participant(participantConfig.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.setParticipant(participantConfig);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public ParticipantConfig updateParticipant(ParticipantId participantId,
      ParticipantConfig.Delta participantDelta) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        Participant participant = super.readParticipant(participantId);
        if (participant == null) {
          LOG.error("Participant " + participantId + " does not exist, cannot be updated");
          return null;
        }
        ParticipantConfig config = participantDelta.mergeInto(participant.getConfig());
        super.setParticipant(config);
        return config;
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  /**
   * Swap a new participant in to serve the replicas of an old (dead) one. The atomicity scope is
   * participant-local and resource-local.
   */
  @Override
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
    List<String> idealStates = _accessor.getChildNames(_keyBuilder.idealStates());
    for (String resourceName : idealStates) {
      HelixLock lock =
          _lockProvider.getLock(_clusterId, Scope.resource(ResourceId.from(resourceName)));
      boolean locked = lock.lock();
      if (locked) {
        try {
          // lock the resource for all ideal state reads and updates
          IdealState idealState = _accessor.getProperty(_keyBuilder.idealState(resourceName));
          if (idealState != null) {
            swapParticipantsInIdealState(idealState, oldParticipantId, newParticipantId);
            PartitionedRebalancerContext context = PartitionedRebalancerContext.from(idealState);
            resourceAccessor.setRebalancerContext(ResourceId.from(resourceName), context);
            _accessor.setProperty(_keyBuilder.idealState(resourceName), idealState);
          }
        } finally {
          lock.unlock();
        }
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  boolean dropParticipant(ParticipantId participantId) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.dropParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public void insertMessagesToParticipant(ParticipantId participantId,
      Map<MessageId, Message> msgMap) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        super.insertMessagesToParticipant(participantId, msgMap);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  public void updateMessageStatus(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        super.updateMessageStatus(participantId, msgMap);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  public void deleteMessagesFromParticipant(ParticipantId participantId, Set<MessageId> msgIdSet) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        super.deleteMessagesFromParticipant(participantId, msgIdSet);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  protected ResourceAccessor resourceAccessor() {
    return new AtomicResourceAccessor(_clusterId, _accessor, _lockProvider);
  }
}
