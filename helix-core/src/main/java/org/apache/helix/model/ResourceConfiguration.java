package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.api.config.ResourceConfig.ResourceType;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

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
 * Persisted configuration properties for a resource
 */
public class ResourceConfiguration extends HelixProperty {
  public enum Fields {
    TYPE
  }

  /**
   * Instantiate for an id
   * @param id resource id
   */
  public ResourceConfiguration(ResourceId id) {
    super(id.stringify());
  }

  /**
   * Get the resource that is rebalanced
   * @return resource id
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getId());
  }

  /**
   * Instantiate from a record
   * @param record configuration properties
   */
  public ResourceConfiguration(ZNRecord record) {
    super(record);
  }

  /**
   * Set the resource type
   * @param type ResourceType type
   */
  public void setType(ResourceType type) {
    _record.setEnumField(Fields.TYPE.toString(), type);
  }

  /**
   * Get the resource type
   * @return ResourceType type
   */
  public ResourceType getType() {
    return _record.getEnumField(Fields.TYPE.toString(), ResourceType.class, ResourceType.DATA);
  }

  /**
   * Get a backward-compatible resource user config
   * @return UserConfig
   */
  public UserConfig getUserConfig() {
    UserConfig userConfig = UserConfig.from(this);
    for (String simpleField : _record.getSimpleFields().keySet()) {
      Optional<Fields> enumField = Enums.getIfPresent(Fields.class, simpleField);
      if (!simpleField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()) {
        userConfig.setSimpleField(simpleField, _record.getSimpleField(simpleField));
      }
    }
    for (String listField : _record.getListFields().keySet()) {
      if (!listField.contains(NamespacedConfig.PREFIX_CHAR + "")) {
        userConfig.setListField(listField, _record.getListField(listField));
      }
    }
    for (String mapField : _record.getMapFields().keySet()) {
      if (!mapField.contains(NamespacedConfig.PREFIX_CHAR + "")) {
        userConfig.setMapField(mapField, _record.getMapField(mapField));
      }
    }
    return userConfig;
  }

  /**
   * Get a RebalancerContext if available
   * @return RebalancerContext, or null
   */
  public RebalancerContext getRebalancerContext(Class<? extends RebalancerContext> clazz) {
    RebalancerConfig config = new RebalancerConfig(this);
    return config.getRebalancerContext(clazz);
  }
}
