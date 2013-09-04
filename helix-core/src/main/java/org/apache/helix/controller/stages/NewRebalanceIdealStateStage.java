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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.rebalancer.NewRebalancer;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Check and invoke custom implementation idealstate rebalancers.<br/>
 * If the resourceConfig has specified className of the customized rebalancer, <br/>
 * the rebalancer will be invoked to re-write the idealstate of the resource<br/>
 */
public class NewRebalanceIdealStateStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(NewRebalanceIdealStateStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    Cluster cluster = event.getAttribute("ClusterDataCache");
    NewCurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());

    // Map<String, IdealState> updatedIdealStates = new HashMap<String, IdealState>();
    for (ResourceId resourceId : cluster.getResourceMap().keySet()) {
      // IdealState currentIdealState = idealStateMap.get(resourceName);
      Resource resource = cluster.getResource(resourceId);
      RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();
      if (rebalancerConfig.getRebalancerMode() == RebalanceMode.USER_DEFINED
          && rebalancerConfig.getRebalancerClassName() != null) {
        String rebalancerClassName = rebalancerConfig.getRebalancerClassName();
        LOG.info("resource " + resourceId + " use idealStateRebalancer " + rebalancerClassName);
        try {
          NewRebalancer balancer =
              (NewRebalancer) (HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());

          // TODO add state model def
          ResourceAssignment resourceAssignment =
              balancer.computeResourceMapping(resource, cluster, null);

          // TODO impl this
          // currentIdealState.updateFromAssignment(resourceAssignment);
          // updatedIdealStates.put(resourceName, currentIdealState);
        } catch (Exception e) {
          LOG.error("Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
        }
      }
    }

    // TODO
    // if (updatedIdealStates.size() > 0) {
      // cache.getIdealStates().putAll(updatedIdealStates);
    // }
  }
}
