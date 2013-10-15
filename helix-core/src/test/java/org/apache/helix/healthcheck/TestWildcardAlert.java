package org.apache.helix.healthcheck;

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

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.ZNRecord;
import org.apache.helix.alerts.AlertValueAndStatus;
import org.apache.helix.api.State;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.mock.participant.MockEspressoHealthReportProvider;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.mbeans.ClusterAlertMBeanCollection;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestWildcardAlert extends ZkIntegrationTestBase {
  public static class TestClusterMBeanObserver extends ClusterMBeanObserver {
    public Map<String, Map<String, Object>> _beanValueMap =
        new ConcurrentHashMap<String, Map<String, Object>>();

    public TestClusterMBeanObserver(String domain) throws InstanceNotFoundException, IOException,
        MalformedObjectNameException, NullPointerException {
      super(domain);
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      try {
        MBeanInfo info = _server.getMBeanInfo(mbsNotification.getMBeanName());
        MBeanAttributeInfo[] infos = info.getAttributes();
        _beanValueMap.put(mbsNotification.getMBeanName().toString(),
            new ConcurrentHashMap<String, Object>());
        for (MBeanAttributeInfo infoItem : infos) {
          Object val = _server.getAttribute(mbsNotification.getMBeanName(), infoItem.getName());
          System.out.println("         " + infoItem.getName() + " : "
              + _server.getAttribute(mbsNotification.getMBeanName(), infoItem.getName())
              + " type : " + infoItem.getType());
          _beanValueMap.get(mbsNotification.getMBeanName().toString()).put(infoItem.getName(), val);
        }
      } catch (Exception e) {
        _logger.error("Error getting bean info, domain=" + _domain, e);
      }
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      _beanValueMap.remove(mbsNotification.getMBeanName().toString());
    }

    public void refresh() throws MalformedObjectNameException, NullPointerException,
        InstanceNotFoundException, IntrospectionException, ReflectionException, IOException,
        AttributeNotFoundException, MBeanException {
      for (String beanName : _beanValueMap.keySet()) {
        ObjectName objName = new ObjectName(beanName);
        MBeanInfo info = _server.getMBeanInfo(objName);
        MBeanAttributeInfo[] infos = info.getAttributes();
        _beanValueMap.put(objName.toString(), new HashMap<String, Object>());
        for (MBeanAttributeInfo infoItem : infos) {
          Object val = _server.getAttribute(objName, infoItem.getName());
          System.out
              .println("         " + infoItem.getName() + " : "
                  + _server.getAttribute(objName, infoItem.getName()) + " type : "
                  + infoItem.getType());
          _beanValueMap.get(objName.toString()).put(infoItem.getName(), val);
        }
      }
    }

  }

  private static final Logger _logger = Logger.getLogger(TestWildcardAlert.class);
  ZkClient _zkClient;
  protected ClusterSetup _setupTool = null;
  protected final String _alertStr =
      "EXP(decay(1)(localhost_*.RestQueryStats@DBName=TestDB0.latency)|EXPAND|SUMEACH)CMP(GREATER)CON(10)";
  protected final String _alertStatusStr = _alertStr; // +" : (*)";
  protected final String _dbName = "TestDB0";

  @BeforeClass()
  public void beforeClass() throws Exception {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    _setupTool = new ClusterSetup(ZK_ADDR);
  }

  @AfterClass
  public void afterClass() {
    _zkClient.close();
  }

  public class WildcardAlertTransition extends MockTransition {
    @Override
    public void doTransition(Message message, NotificationContext context) {
      HelixManager manager = context.getManager();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      State fromState = message.getTypedFromState();
      State toState = message.getTypedToState();
      String instance = message.getTgtName();

      if (fromState.toString().equalsIgnoreCase("SLAVE")
          && toState.toString().equalsIgnoreCase("MASTER")) {
        // add a stat and report to ZK
        // perhaps should keep reporter per instance...
        ParticipantHealthReportCollectorImpl reporter =
            new ParticipantHealthReportCollectorImpl(manager, instance);
        MockEspressoHealthReportProvider provider = new MockEspressoHealthReportProvider();
        reporter.addHealthReportProvider(provider);
        String statName = "latency";
        // using constant as timestamp so that when each partition does this transition,
        // they do not advance timestamp, and no stats double-counted
        String timestamp = "12345";
        provider.setStat(_dbName, statName, "15", timestamp);

        // sleep for random time and see about errors.
        /*
         * Random r = new Random();
         * int x = r.nextInt(30000);
         * try {
         * Thread.sleep(x);
         * } catch (InterruptedException e) {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * }
         */

        reporter.transmitHealthReports();

        /*
         * for (int i = 0; i < 5; i++)
         * {
         * accessor.setProperty(PropertyType.HEALTHREPORT,
         * new ZNRecord("mockAlerts" + i),
         * instance,
         * "mockAlerts");
         * try
         * {
         * Thread.sleep(1000);
         * }
         * catch (InterruptedException e)
         * {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * }
         * }
         */
      }
    }

  }

  @Test()
  public void testWildcardAlert() throws Exception {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START TestWildcardAlert at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start
                                                         // port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes //change back to 5!!!
        3, // replicas //change back to 3!!!
        "MasterSlave", true); // do rebalance

    // enableHealthCheck(clusterName);

    _setupTool.getClusterManagementTool().addAlert(clusterName, _alertStr);
    // _setupTool.getClusterManagementTool().addAlert(clusterName, _alertStr2);

    StartCMResult cmResult =
        TestHelper.startController(clusterName, "controller_0", ZK_ADDR,
            HelixControllerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++) // !!!change back to 5
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] =
          new MockParticipant(clusterName, instanceName, ZK_ADDR, new WildcardAlertTransition());
      participants[i].syncStart();
      // new Thread(participants[i]).start();
    }

    TestClusterMBeanObserver jmxMBeanObserver =
        new TestClusterMBeanObserver(ClusterAlertMBeanCollection.DOMAIN_ALERT);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            ZK_ADDR, clusterName));
    Assert.assertTrue(result);
    Thread.sleep(3000);
    // HealthAggregationTask is supposed to run by a timer every 30s
    // To make sure HealthAggregationTask is run, we invoke it explicitly for this test
    new HealthStatsAggregator(cmResult._manager).aggregate();

    // sleep for a few seconds to give stats stage time to trigger and for bean to trigger
    Thread.sleep(3000);

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // for (int i = 0; i < 1; i++) //change 1 back to 5
    // {
    // String instance = "localhost_" + (12918 + i);
    // String instance = "localhost_12918";
    ZNRecord record = accessor.getProperty(keyBuilder.alertStatus()).getRecord();
    Map<String, Map<String, String>> recMap = record.getMapFields();
    Set<String> keySet = recMap.keySet();
    Map<String, String> alertStatusMap = recMap.get(_alertStatusStr);
    String val = alertStatusMap.get(AlertValueAndStatus.VALUE_NAME);
    boolean fired = Boolean.parseBoolean(alertStatusMap.get(AlertValueAndStatus.FIRED_NAME));
    Assert.assertEquals(Double.parseDouble(val), Double.parseDouble("75.0"));
    Assert.assertTrue(fired);

    // Make sure that the jmxObserver has received all the jmx bean value that is corresponding
    // to the alerts.
    jmxMBeanObserver.refresh();
    Assert.assertTrue(jmxMBeanObserver._beanValueMap.size() >= 1);

    String beanName =
        "HelixAlerts:alert=EXP(decay(1)(localhost_%.RestQueryStats@DBName#TestDB0.latency)|EXPAND|SUMEACH)CMP(GREATER)CON(10)--(%)";
    Assert.assertTrue(jmxMBeanObserver._beanValueMap.containsKey(beanName));

    Map<String, Object> beanValueMap = jmxMBeanObserver._beanValueMap.get(beanName);
    Assert.assertEquals(beanValueMap.size(), 4);
    Assert.assertEquals((beanValueMap.get("AlertFired")), new Integer(1));
    Assert.assertEquals((beanValueMap.get("AlertValue")), new Double(75.0));
    Assert
        .assertEquals(
            (String) (beanValueMap.get("SensorName")),
            "EXP(decay(1)(localhost_%.RestQueryStats@DBName#TestDB0.latency)|EXPAND|SUMEACH)CMP(GREATER)CON(10)--(%)");
    // }

    System.out.println("END TestWildcardAlert at " + new Date(System.currentTimeMillis()));
  }
}
