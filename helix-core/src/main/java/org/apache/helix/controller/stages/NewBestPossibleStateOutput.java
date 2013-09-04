package org.apache.helix.controller.stages;

import java.util.Map;

import org.apache.helix.api.ResourceId;
import org.apache.helix.model.ResourceAssignment;

public class NewBestPossibleStateOutput {

  Map<ResourceId, ResourceAssignment> _resourceAssignmentMap;

  /**
   * @param resourceId
   * @param resourceAssignment
   */
  public void setResourceAssignment(ResourceId resourceId, ResourceAssignment resourceAssignment) {
    _resourceAssignmentMap.put(resourceId, resourceAssignment);
  }
}
