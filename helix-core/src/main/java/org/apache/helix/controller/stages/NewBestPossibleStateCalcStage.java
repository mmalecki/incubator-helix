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

import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Id;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.State;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.controller.rebalancer.AutoRebalancer;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.NewAutoRebalancer;
import org.apache.helix.controller.rebalancer.NewCustomRebalancer;
import org.apache.helix.controller.rebalancer.NewRebalancer;
import org.apache.helix.controller.rebalancer.NewSemiAutoRebalancer;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * For partition compute best possible (instance,state) pair based on
 * IdealState,StateModel,LiveInstance
 */
public class NewBestPossibleStateCalcStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(NewBestPossibleStateCalcStage.class
      .getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    long startTime = System.currentTimeMillis();
    LOG.info("START BestPossibleStateCalcStage.process()");

    NewCurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.toString());
    Map<ResourceId, Resource> resourceMap = event.getAttribute(AttributeName.RESOURCES.toString());
    Cluster cluster = event.getAttribute("ClusterDataCache");

    if (currentStateOutput == null || resourceMap == null || cluster == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires CURRENT_STATE|RESOURCES|DataCache");
    }

    NewBestPossibleStateOutput bestPossibleStateOutput =
        compute(cluster, event, resourceMap, currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.toString(), bestPossibleStateOutput);

    long endTime = System.currentTimeMillis();
    LOG.info("END BestPossibleStateCalcStage.process(). took: " + (endTime - startTime) + " ms");
  }

  // TODO check this
  private NewBestPossibleStateOutput compute(Cluster cluster, ClusterEvent event,
      Map<ResourceId, Resource> resourceMap, NewCurrentStateOutput currentStateOutput) {
    // for each ideal state
    // read the state model def
    // for each resource
    // get the preference list
    // for each instanceName check if its alive then assign a state
    // ClusterDataCache cache = event.getAttribute("ClusterDataCache");

    NewBestPossibleStateOutput output = new NewBestPossibleStateOutput();

    for (ResourceId resourceId : resourceMap.keySet()) {
      LOG.debug("Processing resource:" + resourceId);

      Resource resource = resourceMap.get(resourceId);
      // Ideal state may be gone. In that case we need to get the state model name
      // from the current state
      // IdealState idealState = cache.getIdealState(resourceName);

      Resource existResource = cluster.getResource(resourceId);
      if (existResource == null) {
        // if ideal state is deleted, use an empty one
        LOG.info("resource:" + resourceId + " does not exist anymore");
        // TODO
        // existResource = new Resource();
      }

      RebalancerConfig rebalancerConfig = existResource.getRebalancerConfig();
      NewRebalancer rebalancer = null;
      if (rebalancerConfig.getRebalancerMode() == RebalanceMode.USER_DEFINED
          && rebalancerConfig.getRebalancerClassName() != null) {
        String rebalancerClassName = rebalancerConfig.getRebalancerClassName();
        LOG.info("resource " + resourceId + " use idealStateRebalancer " + rebalancerClassName);
        try {
          rebalancer =
              (NewRebalancer) (HelixUtil.loadClass(getClass(), rebalancerClassName).newInstance());
        } catch (Exception e) {
          LOG.warn("Exception while invoking custom rebalancer class:" + rebalancerClassName, e);
        }
      }
      if (rebalancer == null) {
        if (rebalancerConfig.getRebalancerMode() == RebalanceMode.FULL_AUTO) {
          rebalancer = new NewAutoRebalancer();
        } else if (rebalancerConfig.getRebalancerMode() == RebalanceMode.SEMI_AUTO) {
          rebalancer = new NewSemiAutoRebalancer();
        } else {
          rebalancer = new NewCustomRebalancer();
        }
      }

      // TODO pass state model definition
      ResourceAssignment resourceAssignment =
          rebalancer.computeResourceMapping(resource, cluster, null);

      output.setResourceAssignment(resourceId, resourceAssignment);
    }

    return output;
  }
}
