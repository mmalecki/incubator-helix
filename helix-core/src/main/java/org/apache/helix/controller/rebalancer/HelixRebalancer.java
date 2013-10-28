package org.apache.helix.controller.rebalancer;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.ResourceAssignment;

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
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the resource assignment for a resource in this method.<br/>
 */
public interface HelixRebalancer {
  /**
   * Initialize the rebalancer with a HelixManager if necessary
   * @param manager
   */
  public void init(HelixManager helixManager);

  /**
   * Given an ideal state for a resource and liveness of participants, compute a assignment of
   * instances and states to each partition of a resource. This method provides all the relevant
   * information needed to rebalance a resource. If you need additional information use
   * manager.getAccessor to read and write the cluster data. This allows one to compute the
   * ResourceAssignment according to app-specific requirements.<br/>
   * <br/>
   * Say that you have:<br/>
   * 
   * <pre>
   * class MyRebalancerContext implements RebalancerContext
   * </pre>
   * 
   * as your rebalancer context. To extract it from a RebalancerConfig, do the following:<br/>
   * 
   * <pre>
   * MyRebalancerContext context = rebalancerConfig.getRebalancerContext(MyRebalancerContext.class);
   * </pre>
   * @param rebalancerConfig the properties of the resource for which a mapping will be computed
   * @param cluster complete snapshot of the cluster
   * @param currentState the current states of all partitions
   */
  public ResourceAssignment computeResourceMapping(RebalancerConfig rebalancerConfig,
      Cluster cluster, ResourceCurrentState currentState);
}
