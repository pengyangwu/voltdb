/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestOutOfRangeTimestamps extends RegressionSuite {

    public static final String[] VALID_TIMESTAMPS = {
            "1983-10-31 17:21:21.456987", // just some timestamp
            "1583-01-01 00:00:00.000000", // min valid
            "9999-12-31 23:23:59.999999"  // max valid
    };

    public static final String[] INVALID_TIMESTAMPS = {
            "1582-12-31 23:23:59.999999",  // just below min valid
            "10000-01-01 00:00:00.000000", // just above max valid
            "1480-10-31 17:17:39.948257",  // too early
            "10123-04-21 21:09:34.123059"  // too late
    };

    public void testIt() throws Exception {
        Client client = getClient();

        int i = 0;
        for (String ts : VALID_TIMESTAMPS) {
            String createProcStmt = "create procedure myproc_" + i + " as insert into t values(" + i + ", '" + ts + "');";

            client.callProcedure("@AdHoc", createProcStmt);
            ++i;
        }

        for (String ts : INVALID_TIMESTAMPS) {
            String createProcStmt = "create procedure myproc_" + i + " as insert into t values(" + i + ", '" + ts + "');";
            verifyStmtFails(client, createProcStmt, "timestamp value is outside of the supported range");
            ++i;
        }

    }

    public TestOutOfRangeTimestamps(String name) {
        super(name);

    }

    static public Test suite() throws Exception {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestOutOfRangeTimestamps.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String ddl =
                "create table t ("
                + "  pk integer not null primary key,"
                + "  ts timestamp"
                + ");";

        project.addLiteralSchema(ddl);
        project.setUseDDLSchema(true);

        config = new LocalCluster("out-of-range-timestamps-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success= config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
