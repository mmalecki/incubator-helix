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

import org.apache.helix.HelixConstants.StateModelToken;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.ResourceConfig.ResourceType;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.context.CustomRebalancerContext;
import org.apache.helix.controller.rebalancer.context.PartitionedRebalancerContext;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.controller.rebalancer.context.SemiAutoRebalancerContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.log4j.Logger;

public class ResourceAccessor {
  private static final Logger LOG = Logger.getLogger(ResourceAccessor.class);
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  public ResourceAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * Read a single snapshot of a resource
   * @param resourceId the resource id to read
   * @return Resource or null if not present
   */
  public Resource readResource(ResourceId resourceId) {
    ResourceConfiguration config =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealState(resourceId.stringify()));

    if (config == null && idealState == null) {
      LOG.error("Resource " + resourceId + " not present on the cluster");
      return null;
    }

    ExternalView externalView =
        _accessor.getProperty(_keyBuilder.externalView(resourceId.stringify()));
    ResourceAssignment resourceAssignment =
        _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
    return createResource(resourceId, config, idealState, externalView, resourceAssignment);
  }

  /**
   * Update a resource configuration
   * @param resourceId the resource id to update
   * @param resourceDelta changes to the resource
   * @return ResourceConfig, or null if the resource is not persisted
   */
  public ResourceConfig updateResource(ResourceId resourceId, ResourceConfig.Delta resourceDelta) {
    Resource resource = readResource(resourceId);
    if (resource == null) {
      LOG.error("Resource " + resourceId + " does not exist, cannot be updated");
      return null;
    }
    ResourceConfig config = resourceDelta.mergeInto(resource.getConfig());
    setResource(config);
    return config;
  }

  /**
   * save resource assignment
   * @param resourceId
   * @param resourceAssignment
   */
  public void setResourceAssignment(ResourceId resourceId, ResourceAssignment resourceAssignment) {
    _accessor.setProperty(_keyBuilder.resourceAssignment(resourceId.stringify()),
        resourceAssignment);
  }

  /**
   * get resource assignment
   * @param resourceId
   * @return resource assignment or null
   */
  public ResourceAssignment getResourceAssignment(ResourceId resourceId) {
    return _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
  }

  /**
   * Set a physical resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @param configuration
   */
  void setConfiguration(ResourceId resourceId, ResourceConfiguration configuration) {
    _accessor.setProperty(_keyBuilder.resourceConfig(resourceId.stringify()), configuration);
    // also set an ideal state if the resource supports it
    RebalancerConfig rebalancerConfig = new RebalancerConfig(configuration);
    IdealState idealState =
        rebalancerConfigToIdealState(rebalancerConfig, configuration.getBucketSize(),
            configuration.getBatchMessageMode());
    if (idealState != null) {
      _accessor.setProperty(_keyBuilder.idealState(resourceId.stringify()), idealState);
    }
  }

  /**
   * Set the context of the rebalancer. This includes all properties required for rebalancing this
   * resource
   * @param resourceId the resource to update
   * @param context the new rebalancer context
   * @return true if the context was set, false otherwise
   */
  public boolean setRebalancerContext(ResourceId resourceId, RebalancerContext context) {
    RebalancerConfig config = new RebalancerConfig(context);
    ResourceConfiguration resourceConfig = new ResourceConfiguration(resourceId);
    resourceConfig.addNamespacedConfig(config.toNamespacedConfig());
    return _accessor.updateProperty(_keyBuilder.resourceConfig(resourceId.stringify()),
        resourceConfig);
  }

  /**
   * Read the user config of the resource
   * @param resourceId the resource to to look up
   * @return UserConfig, or null
   */
  public UserConfig readUserConfig(ResourceId resourceId) {
    ResourceConfiguration resourceConfig =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return resourceConfig != null ? UserConfig.from(resourceConfig) : null;
  }

  /**
   * Read the rebalancer config of the resource
   * @param resourceId the resource to to look up
   * @return RebalancerConfig, or null
   */
  public RebalancerConfig readRebalancerConfig(ResourceId resourceId) {
    ResourceConfiguration resourceConfig =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return resourceConfig != null ? RebalancerConfig.from(resourceConfig) : null;
  }

  /**
   * Set the user config of the resource, overwriting existing user configs
   * @param resourceId the resource to update
   * @param userConfig the new user config
   * @return true if the user config was set, false otherwise
   */
  public boolean setUserConfig(ResourceId resourceId, UserConfig userConfig) {
    ResourceConfig.Delta delta = new ResourceConfig.Delta(resourceId).setUserConfig(userConfig);
    return updateResource(resourceId, delta) != null;
  }

  /**
   * Add user configuration to the existing resource user configuration. Overwrites properties with
   * the same key
   * @param resourceId the resource to update
   * @param userConfig the user config key-value pairs to add
   * @return true if the user config was updated, false otherwise
   */
  public boolean updateUserConfig(ResourceId resourceId, UserConfig userConfig) {
    ResourceConfiguration resourceConfig = new ResourceConfiguration(resourceId);
    resourceConfig.addNamespacedConfig(userConfig);
    return _accessor.updateProperty(_keyBuilder.resourceConfig(resourceId.stringify()),
        resourceConfig);
  }

  /**
   * Persist an existing resource's logical configuration
   * @param resourceConfig logical resource configuration
   * @return true if resource is set, false otherwise
   */
  public boolean setResource(ResourceConfig resourceConfig) {
    if (resourceConfig == null || resourceConfig.getRebalancerConfig() == null) {
      LOG.error("Resource not fully defined with a rebalancer context");
      return false;
    }
    ResourceId resourceId = resourceConfig.getId();
    ResourceConfiguration config = new ResourceConfiguration(resourceId);
    config.addNamespacedConfig(resourceConfig.getUserConfig());
    config.addNamespacedConfig(resourceConfig.getRebalancerConfig().toNamespacedConfig());
    config.setBucketSize(resourceConfig.getBucketSize());
    config.setBatchMessageMode(resourceConfig.getBatchMessageMode());
    setConfiguration(resourceId, config);
    return true;
  }

  /**
   * Get a resource configuration, which may include user-defined configuration, as well as
   * rebalancer configuration
   * @param resourceId
   * @return configuration
   */
  public void getConfiguration(ResourceId resourceId) {
    _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
  }

  /**
   * set external view of a resource
   * @param resourceId
   * @param extView
   */
  public void setExternalView(ResourceId resourceId, ExternalView extView) {
    _accessor.setProperty(_keyBuilder.externalView(resourceId.stringify()), extView);
  }

  /**
   * drop external view of a resource
   * @param resourceId
   */
  public void dropExternalView(ResourceId resourceId) {
    _accessor.removeProperty(_keyBuilder.externalView(resourceId.stringify()));
  }

  /**
   * Get an ideal state from a rebalancer config if the resource is partitioned
   * @param config RebalancerConfig instance
   * @param bucketSize bucket size to use
   * @param batchMessageMode true if batch messaging allowed, false otherwise
   * @return IdealState, or null
   */
  static IdealState rebalancerConfigToIdealState(RebalancerConfig config, int bucketSize,
      boolean batchMessageMode) {
    PartitionedRebalancerContext partitionedContext =
        config.getRebalancerContext(PartitionedRebalancerContext.class);
    if (partitionedContext != null) {
      IdealState idealState = new IdealState(partitionedContext.getResourceId());
      idealState.setRebalanceMode(partitionedContext.getRebalanceMode());
      idealState.setRebalancerRef(partitionedContext.getRebalancerRef());
      String replicas = null;
      if (partitionedContext.anyLiveParticipant()) {
        replicas = StateModelToken.ANY_LIVEINSTANCE.toString();
      } else {
        replicas = Integer.toString(partitionedContext.getReplicaCount());
      }
      idealState.setReplicas(replicas);
      idealState.setNumPartitions(partitionedContext.getPartitionSet().size());
      idealState.setInstanceGroupTag(partitionedContext.getParticipantGroupTag());
      idealState.setMaxPartitionsPerInstance(partitionedContext.getMaxPartitionsPerParticipant());
      idealState.setStateModelDefId(partitionedContext.getStateModelDefId());
      idealState.setStateModelFactoryId(partitionedContext.getStateModelFactoryId());
      idealState.setBucketSize(bucketSize);
      idealState.setBatchMessageMode(batchMessageMode);
      if (partitionedContext.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        SemiAutoRebalancerContext semiAutoContext =
            config.getRebalancerContext(SemiAutoRebalancerContext.class);
        for (PartitionId partitionId : semiAutoContext.getPartitionSet()) {
          idealState.setPreferenceList(partitionId, semiAutoContext.getPreferenceList(partitionId));
        }
      } else if (partitionedContext.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        CustomRebalancerContext customContext =
            config.getRebalancerContext(CustomRebalancerContext.class);
        for (PartitionId partitionId : customContext.getPartitionSet()) {
          idealState.setParticipantStateMap(partitionId,
              customContext.getPreferenceMap(partitionId));
        }
      }
      return idealState;
    }
    return null;
  }

  /**
   * Create a resource snapshot instance from the physical model
   * @param resourceId the resource id
   * @param resourceConfiguration physical resource configuration
   * @param idealState ideal state of the resource
   * @param externalView external view of the resource
   * @param resourceAssignment current resource assignment
   * @return Resource
   */
  static Resource createResource(ResourceId resourceId,
      ResourceConfiguration resourceConfiguration, IdealState idealState,
      ExternalView externalView, ResourceAssignment resourceAssignment) {
    UserConfig userConfig;
    ResourceType type = ResourceType.DATA;
    if (resourceConfiguration != null) {
      userConfig = UserConfig.from(resourceConfiguration);
      type = resourceConfiguration.getType();
    } else {
      userConfig = new UserConfig(Scope.resource(resourceId));
    }
    int bucketSize = 0;
    boolean batchMessageMode = false;
    RebalancerContext rebalancerContext;
    if (idealState != null) {
      rebalancerContext = PartitionedRebalancerContext.from(idealState);
      bucketSize = idealState.getBucketSize();
      batchMessageMode = idealState.getBatchMessageMode();
    } else {
      if (resourceConfiguration != null) {
        bucketSize = resourceConfiguration.getBucketSize();
        batchMessageMode = resourceConfiguration.getBatchMessageMode();
        RebalancerConfig rebalancerConfig = new RebalancerConfig(resourceConfiguration);
        rebalancerContext = rebalancerConfig.getRebalancerContext(RebalancerContext.class);
      } else {
        rebalancerContext = new PartitionedRebalancerContext(RebalanceMode.NONE);
      }
    }
    return new Resource(resourceId, type, idealState, resourceAssignment, externalView,
        rebalancerContext, userConfig, bucketSize, batchMessageMode);
  }
}