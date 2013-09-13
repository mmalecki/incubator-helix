package org.apache.helix.controller.rebalancer;

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

import org.apache.helix.api.Cluster;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.ResourceAssignment;

/**
 * Arbitrary configurable rebalancer interface.
 * @see {@link NewUserDefinedRebalancer} for an interface with a plugged-in class
 */
interface NewRebalancer<T extends RebalancerConfig> {

  /**
   * Given a resource, existing mapping, and liveness of resources, compute a new mapping of
   * resources.
   * @param rebalancerConfig resource properties used by the rebalancer
   * @param cluster a snapshot of the entire cluster state
   * @param currentState a combination of the current states and pending current states
   */
  ResourceAssignment computeResourceMapping(final T rebalancerConfig, final Cluster cluster,
      final ResourceCurrentState currentState);
}
