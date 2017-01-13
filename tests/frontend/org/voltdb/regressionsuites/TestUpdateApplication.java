/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableTestHelpers;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;

/**
 * Tests a mix of multi-partition and single partition procedures on a
 * mix of replicated and partititioned tables on a mix of single-site and
 * multi-site VoltDB instances.
 *
 */
public class TestUpdateApplication extends AdhocDDLTestBase {

    static Class<?>[] PROC_CLASSES = { org.voltdb_testprocs.updateclasses.testImportProc.class,
        org.voltdb_testprocs.updateclasses.testCreateProcFromClassProc.class };

    static Class<?>[] EXTRA_CLASSES = { org.voltdb_testprocs.updateclasses.NoMeaningClass.class };

    @Test
    public void testBasicWithDDLStmts() throws Exception {
        System.out.println("\n\n-----\n testBasic with create procedure statment \n-----\n\n");

        String pathToCatalog = Configuration.getPathToCatalogForTest("updateclasses.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("updateclasses.xml");
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("-- Don't care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        // This is maybe cheating a little bit?
        InMemoryJarfile jarfile = new InMemoryJarfile();
        for (Class<?> clazz : PROC_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        for (Class<?> clazz : EXTRA_CLASSES) {
            VoltCompiler comp = new VoltCompiler();
            comp.addClassToJar(jarfile, clazz);
        }
        // Add a deployment file just to have something other than classes in the jar
        jarfile.put("deployment.xml", new File(pathToDeployment));

        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = pathToCatalog;
            config.m_pathToDeployment = pathToDeployment;
            startSystem(config);

            ClientResponse resp;
            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            System.out.println(resp.getResults()[0]);
            // New cluster, you're like summer vacation...
            assertEquals(0, resp.getResults()[0].getRowCount());
            assertFalse(VoltTableTestHelpers.moveToMatchingRow(resp.getResults()[0], "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            boolean threw = false;
            try {
                resp = m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("was not found"));
                threw = true;
            }
            assertTrue(threw);

            // With the third extra DDL statement
            //String stmt = "load classes updateclasses.jar;\n";
            String stmt = "";
            stmt += "create procedure from class " + PROC_CLASSES[0].getCanonicalName() + "";

            String[] jarIdentifiers = new String[]{"xxx.jar"};
            byte[][] jarBytes = new byte[1][];
            jarBytes[0] = jarfile.getFullJarBytes();

            resp = m_client.callProcedure("@UpdateApplication", stmt, jarIdentifiers, jarBytes, null);
            System.out.println(((ClientResponseImpl)resp).toJSONString());

            resp = m_client.callProcedure("@SystemCatalog", "CLASSES");
            VoltTable results = resp.getResults()[0];
            System.out.println(results);
            assertEquals(3, results.getRowCount());
            assertTrue(VoltTableTestHelpers.moveToMatchingRow(results, "CLASS_NAME",
                        PROC_CLASSES[0].getCanonicalName()));
            assertEquals(1L, results.getLong("VOLT_PROCEDURE"));
            assertEquals(1L, results.getLong("ACTIVE_PROC"));

            // Verify the new class as a stored procedure
            resp = m_client.callProcedure(PROC_CLASSES[0].getSimpleName());
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            results = resp.getResults()[0];
            assertEquals(10L, results.asScalarLong());
        }
        finally {
            teardownSystem();
        }
    }
}
