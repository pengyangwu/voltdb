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

package org.voltdb.compiler;

import java.util.List;

import org.voltdb.AuthSystem;
import org.voltdb.utils.Encoder;

public class CatalogChangeWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -5257248292283453286L;

    public static class CatalogChangeParameters {
        public final String[] ddlStmts;
        public final String jarIdentifier;
        // The bytes for the catalog operation, if any.  May be null in all cases
        // For @UpdateApplicationCatalog, this will contain the compiled catalog jarfile bytes
        // For @UpdateClasses, this will contain the class jarfile bytes
        // For @AdHoc DDL work, this will be null
        public final byte[] operationBytes;
        // The string for the catalog operation, if any.  May be null in all cases
        // For @UpdateApplicationCatalog, this will contain the deployment string to apply
        // For @UpdateClasses, this will contain the class deletion patterns
        // For @AdHoc DDL work, this will be null
        public final String operationString;

        public CatalogChangeParameters (String[] stmts, String identifier, byte[] operationBytes, String operationString) {
            this.ddlStmts = stmts;
            this.jarIdentifier = identifier;
            this.operationBytes = operationBytes;
            this.operationString = operationString;
        }
    }

    public CatalogChangeParameters ccParams;
    final byte[] replayHashOverride;
    public final long replayTxnId;
    public final long replayUniqueId;

    public CatalogChangeWork(
            long replySiteId,
            long clientHandle, long connectionId, String hostname, boolean adminConnection, Object clientData,
            CatalogChangeParameters ccParams,
            String invocationName, boolean onReplica, boolean useAdhocDDL,
            AsyncCompilerWorkCompletionHandler completionHandler,
            AuthSystem.AuthUser user, byte[] replayHashOverride,
            long replayTxnId, long replayUniqeuId)
    {
        super(replySiteId, false, clientHandle, connectionId, hostname,
              adminConnection, clientData, invocationName,
              onReplica, useAdhocDDL,
              completionHandler, user);
        this.ccParams = ccParams;
        this.replayHashOverride = replayHashOverride;
        this.replayTxnId = replayTxnId;
        this.replayUniqueId = replayUniqeuId;
    }

    /**
     * To process adhoc DDL, we want to convert the AdHocPlannerWork we received from the
     * ClientInterface into a CatalogChangeWork object for the AsyncCompilerAgentHelper to
     * grind on.
     */
    public CatalogChangeWork(AdHocPlannerWork adhocDDL)
    {
        super(adhocDDL.replySiteId,
              adhocDDL.shouldShutdown,
              adhocDDL.clientHandle,
              adhocDDL.connectionId,
              adhocDDL.hostname,
              adhocDDL.adminConnection,
              adhocDDL.clientData,
              adhocDDL.invocationName,
              adhocDDL.onReplica,
              adhocDDL.useAdhocDDL,
              adhocDDL.completionHandler,
              adhocDDL.user);
        // AsyncCompilerAgentHelper will fill in the current catalog bytes later.
        // Ditto for deployment string
        this.ccParams = new CatalogChangeParameters(adhocDDL.sqlStatements, null, null, null);

        this.replayHashOverride = null;
        this.replayTxnId = -1L;
        this.replayUniqueId = -1L;
    }

    public boolean isForReplay()
    {
        return replayHashOverride != null;
    }

    public static CatalogChangeParameters fromParams(String procName, Object[] paramArray) {
        if (paramArray == null) return null;

        String[] ddlStmts = null;
        String jarIdentifier = null;
        byte[] catalogBytes = null;
        String operationString = null;

        // default catalogBytes to null, when passed along, will tell the
        // catalog change planner that we want to use the current catalog.
        if ("@UpdateApplicationCatalog".equals(procName) || "@UpdateClasses".equals(procName)) {
            Object catalogObj = paramArray[0];
            if (catalogObj != null) {
                if (catalogObj instanceof String) {
                    // treat an empty string as no catalog provided
                    String catalogString = (String) catalogObj;
                    if (!catalogString.isEmpty()) {
                        catalogBytes = Encoder.hexDecode(catalogString);
                    }
                } else if (catalogObj instanceof byte[]) {
                    // treat an empty array as no catalog provided
                    byte[] catalogArr = (byte[]) catalogObj;
                    if (catalogArr.length != 0) {
                        catalogBytes = catalogArr;
                    }
                }
            }
            operationString = (String) paramArray[1];
        }
        else if ("@UpdateApplication".equals(procName)) {
            // input parameters has been checked
            String sql = null;
            if (paramArray[0] != null) {
                sql = (String) paramArray[0];
            }
            // TODO(xin): SQL String to String[] stmts
            ddlStmts = new String[]{sql};

            if (paramArray[1] != null) {
                List<java.util.Map.Entry<String, byte[]>> jarInfoList = (List<java.util.Map.Entry<String, byte[]>>) paramArray[1];
                java.util.Map.Entry<String, byte[]> jarInfo = jarInfoList.iterator().next();

                jarIdentifier = jarInfo.getKey();
                catalogBytes = jarInfo.getValue();
            }
            if (paramArray[2] != null) {
                // classes delete pattern strings
                operationString = (String) paramArray[2];
            }
        }

        return new CatalogChangeParameters(ddlStmts, jarIdentifier, catalogBytes, operationString);
    }
}
