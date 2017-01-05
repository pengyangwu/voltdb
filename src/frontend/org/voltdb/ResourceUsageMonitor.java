/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import com.google_voltpatches.common.collect.ImmutableSet;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.deploymentfile.ResourceMonitorType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.importer.ChannelChangeCallback;
import org.voltdb.importer.ChannelDistributer;
import org.voltdb.importer.ImporterChannelAssignment;
import org.voltdb.importer.VersionedOperationMode;
import org.voltdb.snmp.FaultFacility;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.snmp.ThresholdType;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to periodically check if the server's resource utilization is above the configured limits
 * and pause the server.
 */
public class ResourceUsageMonitor implements Runnable, ChannelChangeCallback
{
    private static final VoltLogger m_logger = new VoltLogger("HOST");
    private static final String DR_ROLE_STATS_CHANNEL = "drRoleStats";


    private String m_rssLimitStr;
    private long m_rssLimit;
    private int m_resourceCheckInterval;
    private DiskResourceChecker m_diskLimitConfig;
    private boolean m_snmpMemoryTrapSent = false;
    private SnmpTrapSender m_snmpTrapSender;
    private String m_snmpRssLimitStr;
    private long m_snmpRssLimit;
    private ThresholdType m_snmpRssCriteria;
    private boolean m_isDREnabled;
    private ChannelDistributer m_distributer;
    private String m_clusterTag;
    private String m_distributerDesignation;
    private URI m_drRoleStatsURI;
    private Set<URI> m_channels;
    private AtomicBoolean m_isDRTrapSender = new AtomicBoolean(false);

    public ResourceUsageMonitor(SystemSettingsType systemSettings, SnmpTrapSender snmpTrapSender, ChannelDistributer distributer, boolean isDREnabled)
    {
        if (systemSettings == null || systemSettings.getResourcemonitor() == null) {
            return;
        }

        ResourceMonitorType config = systemSettings.getResourcemonitor();
        m_resourceCheckInterval = config.getFrequency();

        if (config.getMemorylimit() != null) {
            m_rssLimitStr = config.getMemorylimit().getSize().trim();
            // configured value is in GB. Convert it to bytes
            double dblLimit = getMemoryLimitSize(m_rssLimitStr);
            m_rssLimit = Double.valueOf(dblLimit).longValue();
        }

        m_diskLimitConfig = new DiskResourceChecker(systemSettings.getResourcemonitor().getDisklimit(), snmpTrapSender);

        // for snmp trap
        m_snmpTrapSender = snmpTrapSender;
        if (config.getMemorylimit() != null) {
            m_snmpRssLimitStr = config.getMemorylimit().getAlert().trim();
            // configured value is in GB. Convert it to bytes
            double dblLimit = getMemoryLimitSize(m_snmpRssLimitStr);
            m_snmpRssLimit = Double.valueOf(dblLimit).longValue();
            m_snmpRssCriteria = m_snmpRssLimitStr.endsWith("%") ? ThresholdType.PERCENT : ThresholdType.LIMIT;
        }

        m_isDREnabled = isDREnabled;
        if (m_isDREnabled) {
            m_distributer = distributer;
            m_clusterTag = m_distributer.getClusterTag();
            m_distributerDesignation = "DRRoleStatsConsumer";
            m_drRoleStatsURI = URI.create(String.format("x-drrolestatsconsumer://" + m_distributer.getClusterTag()));
            m_channels = ImmutableSet.of(m_drRoleStatsURI);
        }
    }

    public void registerForChannelCallbacks() {
        if (m_isDREnabled) {
            m_distributer.registerCallback(m_distributerDesignation, this);
            m_distributer.registerChannels(m_distributerDesignation, m_channels);
        }
    }

    public boolean hasResourceLimitsConfigured()
    {
        return ((m_rssLimit > 0 || m_snmpRssLimit > 0 || (m_diskLimitConfig!=null && m_diskLimitConfig.hasLimitsConfigured()))
                && m_resourceCheckInterval > 0);
    }

    public int getResourceCheckInterval()
    {
        return m_resourceCheckInterval;
    }

    public void logResourceLimitConfigurationInfo()
    {
        if (hasResourceLimitsConfigured()) {
            m_logger.info("Resource limit monitoring configured to run every " + m_resourceCheckInterval + " seconds");
            if (m_rssLimit > 0) {
                m_logger.info("RSS limit: "  + getRssLimitLogString(m_rssLimit, m_rssLimitStr));
            }
            if (MiscUtils.isPro() && m_snmpRssLimit > 0) {
                m_logger.info("RSS SNMP notification limit: "  + getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr));
            }
            if (m_diskLimitConfig!=null) {
                m_diskLimitConfig.logConfiguredLimits();
            }
        } else {
            m_logger.info("No resource usage limit monitoring configured");
        }
    }

    private String getRssLimitLogString(long rssLimit, String rssLimitStr)
    {
        String rssWithUnit = getValueWithUnit(rssLimit);
        return (rssLimitStr.endsWith("%") ?
                rssLimitStr + " (" +  rssWithUnit + ")" : rssWithUnit);
    }

    @Override
    public void run()
    {
        System.err.println("XXX Res runs " + System.currentTimeMillis());

        if (getClusterOperationMode() != OperationMode.RUNNING) {
            return;
        }

        if (hasResourceLimitsConfigured() && (isOverMemoryLimit() || m_diskLimitConfig.isOverLimitConfiguration())) {
            SyncCallback cb = new SyncCallback();
            if (getConnectionHandler().callProcedure(getInternalUser(), true, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@Pause")) {
                try {
                    cb.waitForResponse();
                } catch (InterruptedException e) {
                    m_logger.error("Interrupted while pausing cluster for resource overusage", e);
                }
                ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
                if (r.getStatus() != ClientResponse.SUCCESS) {
                    m_logger.error("Unable to pause cluster for resource overusage: " + r.getStatusString());
                }
            } else {
                m_logger.error("Unable to pause cluster for resource overusage: failed to invoke @Pause");
            }
        }

        if (m_isDREnabled && m_isDRTrapSender.get()) {
            System.err.println("XXX in run, I'm the trap sender.");
            SyncCallback cb = new SyncCallback();
            if (getConnectionHandler().callProcedure(getInternalUser(), true, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@Statistics", "DRROLE", 0)) {
                try {
                    cb.waitForResponse();
                    ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
                    VoltTable stats = r.getResults()[0];
                    while (stats.advanceRow()) {
                        String role = stats.getString(DRRoleStats.CN_ROLE);
                        String state = stats.getString(DRRoleStats.CN_STATE);
                        System.err.println("XXX stats: " + role + " : " + state + " : " + stats.getLong(DRRoleStats.CN_REMOTE_CLUSTER_ID));
                        if (role == null || role.trim().isEmpty() || state == null || state.trim().isEmpty() || !state.equals("STOPPED")) {
                            continue;
                        }
                        m_snmpTrapSender.drRelationship(String.format("DR relationship failure, role %s is stopped on cluster %s.", stats.getString(DRRoleStats.CN_ROLE), m_clusterTag));
                    }

                } catch (InterruptedException e) {
                    m_logger.error("Interrupted while pausing cluster for resource overusage", e);
                }
            } else {
                m_logger.error("Unable to query for DRROLE statistics");
            }
        } else {
            System.err.println("XXX Ah, in run, not the trap sender.");
        }

    }

    @Override
    public void onChange(ImporterChannelAssignment assignment) {
        if (assignment.getAssigned().contains(m_drRoleStatsURI)) {
            m_isDRTrapSender.set(true);
            System.err.println("XXX on change, I'm the trap sender.");
        } else {
            m_isDRTrapSender.set(false);
            System.err.println("XXX on change, not the trap sender");
        }


        // TODO: need to think about version?

        System.err.println("XXX on change");

        for (URI removed: assignment.getRemoved()) {
            System.err.println("XXX removed " + removed);
        }

        for (URI added: assignment.getAdded()) {
            System.err.println("XXX added " + added);
        }

        for (URI assigned: assignment.getAssigned()) {
            System.err.println("XXX assigned " + assigned);
        }
    }

    @Override
    public void onClusterStateChange(VersionedOperationMode mode) {}

    private OperationMode getClusterOperationMode()
    {
        return VoltDB.instance().getMode();
    }

    private InternalConnectionHandler getConnectionHandler()
    {
        return VoltDB.instance().getClientInterface().getInternalConnectionHandler();
    }

    private AuthUser getInternalUser()
    {
        return VoltDB.instance().getCatalogContext().authSystem.getInternalAdminUser();
    }

    private boolean isOverMemoryLimit()
    {
        if (m_rssLimit<=0 && m_snmpRssLimit<=0) {
            return false;
        }

        Datum datum = SystemStatsCollector.getRecentSample();
        if (datum == null) { // this will be null if stats has not run yet
            m_logger.warn("No stats are available from stats collector. Skipping resource check.");
            return false;
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("RSS=" + datum.rss + " Configured rss limit=" + m_rssLimit +
                    " Configured SNMP rss limit=" + m_snmpRssLimit);
        }

        if (MiscUtils.isPro()) {
            if (m_snmpRssLimit > 0 && datum.rss >= m_snmpRssLimit) {
                if (!m_snmpMemoryTrapSent) {
                    m_snmpTrapSender.resource(m_snmpRssCriteria, FaultFacility.MEMORY, m_snmpRssLimit, datum.rss,
                            String.format("SNMP resource limit exceeded. RSS limit %s on %s. Current RSS size %s.",
                                    getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr),
                                    CoreUtils.getHostnameOrAddress(), getValueWithUnit(datum.rss)));
                    m_snmpMemoryTrapSent = true;
                }
            } else {
                if (m_snmpRssLimit > 0 && m_snmpMemoryTrapSent) {
                    m_snmpTrapSender.resourceClear(m_snmpRssCriteria, FaultFacility.MEMORY, m_snmpRssLimit, datum.rss,
                            String.format("SNMP resource limit cleared. RSS limit %s on %s. Current RSS size %s.",
                                    getRssLimitLogString(m_snmpRssLimit, m_snmpRssLimitStr),
                                    CoreUtils.getHostnameOrAddress(), getValueWithUnit(datum.rss)));
                    m_snmpMemoryTrapSent = false;
                }
            }
        }

        if (m_rssLimit > 0 && datum.rss >= m_rssLimit) {
            m_logger.error(String.format(
                    "Resource limit exceeded. RSS limit %s on %s. Setting database to read-only. " +
                    "Use \"voltadmin resume\" command once resource constraint is corrected.",
                    getRssLimitLogString(m_rssLimit,m_rssLimitStr), CoreUtils.getHostnameOrAddress()));
            m_logger.error(String.format("Resource limit exceeded. Current RSS size %s.", getValueWithUnit(datum.rss)));
            return true;
        } else {
            return false;
        }
    }

    public static String getValueWithUnit(long value)
    {
        if (value >= 1073741824L) {
            return String.format("%.2f GB", (value/1073741824.0));
        } else if (value >= 1048576) {
            return String.format("%.2f MB", (value/1048576.0));
        } else {
            return value + " bytes";
        }
    }

    // package-private for junit
    double getMemoryLimitSize(String sizeStr)
    {
        if (sizeStr==null || sizeStr.length()==0) {
            return 0;
        }

        try {
            if (sizeStr.charAt(sizeStr.length()-1)=='%') { // size as a percentage of total available memory
                int perc = Integer.parseInt(sizeStr.substring(0, sizeStr.length()-1));
                if (perc<0 || perc > 99) {
                    throw new IllegalArgumentException("Invalid memory limit percentage: " + sizeStr);
                }
                return PlatformProperties.getPlatformProperties().ramInMegabytes*1048576L*perc/100.0;
            } else { // size in GB
                double size = Double.parseDouble(sizeStr)*1073741824L;
                if (size<0) {
                    throw new IllegalArgumentException("Invalid memory limit value: " + sizeStr);
                }
                return size;
            }
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("Invalid memory limit value " + sizeStr +
                    ". Memory limit must be configued as a percentage of total available memory or as GB value");
        }
    }
}
