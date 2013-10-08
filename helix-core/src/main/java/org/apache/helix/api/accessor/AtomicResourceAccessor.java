package org.apache.helix.api.accessor;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.HelixLockable;
import org.apache.log4j.Logger;

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

/**
 * An atomic version of the ResourceAccessor. If atomic operations are required, use instances of
 * this class. Atomicity is not guaranteed when using instances of ResourceAccessor alongside
 * instances of this class. Furthermore, depending on the semantics of the lock, lock acquisition
 * may fail, in which case users should handle the return value of each function if necessary.
 */
public class AtomicResourceAccessor extends ResourceAccessor {
  private static final Logger LOG = Logger.getLogger(AtomicResourceAccessor.class);

  private final ClusterId _clusterId;
  private final HelixLockable _lockProvider;

  public AtomicResourceAccessor(ClusterId clusterId, HelixDataAccessor accessor,
      HelixLockable lockProvider) {
    super(accessor);
    _clusterId = clusterId;
    _lockProvider = lockProvider;
  }

  @Override
  public Resource readResource(ResourceId resourceId) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.readResource(resourceId);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public ResourceConfig updateResource(ResourceId resourceId, ResourceConfig.Delta resourceDelta) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        Resource resource = super.readResource(resourceId);
        if (resource == null) {
          LOG.error("Resource " + resourceId + " does not exist, cannot be updated");
          return null;
        }
        ResourceConfig config = resourceDelta.mergeInto(resource.getConfig());
        super.setResource(config);
        return config;
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public boolean setRebalancerContext(ResourceId resourceId, RebalancerContext context) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.setRebalancerContext(resourceId, context);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean setResource(ResourceConfig resourceConfig) {
    if (resourceConfig == null) {
      LOG.error("resource config cannot be null");
      return false;
    }
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resourceConfig.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.setResource(resourceConfig);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean generateDefaultAssignment(ResourceId resourceId, int replicaCount,
      String participantGroupTag) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return super.generateDefaultAssignment(resourceId, replicaCount, participantGroupTag);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }
}
