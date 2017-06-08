/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.quotas.FileArchiverNotifierImpl.SnapshotWithSize;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.FamilyFiles;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Test class for {@link FileArchiverNotifierImpl}.
 */
@Category(MediumTests.class)
public class TestFileArchiverNotifierImpl {
  private static final Log LOG = LogFactory.getLog(TestFileArchiverNotifierImpl.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong();

  @Rule
  public TestName testName = new TestName();

  private Connection conn;
  private Admin admin;
  private SpaceQuotaHelperForTests helper;
  private FileSystem fs;
  private Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    // Clean up the compacted files faster than normal (15s instead of 2mins)
    conf.setInt("hbase.hfile.compaction.discharger.interval", 15 * 1000);
    // Prevent the SnapshotQuotaObserverChore from running
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_DELAY_KEY, 60 * 60 * 1000);
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_PERIOD_KEY, 60 * 60 * 1000);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    conn = TEST_UTIL.getConnection();
    admin = TEST_UTIL.getAdmin();
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    helper.removeAllQuotas(conn);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
  }
  
  @Test
  public void testSnapshotSizePersistence() throws IOException {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(testName.getMethodName());
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }
    HTableDescriptor desc = TableDescriptorBuilder.newBuilder(tn)
        .addFamily(new HColumnDescriptor(QuotaTableUtil.QUOTA_FAMILY_USAGE)).build();
    admin.createTable(desc);

    FileArchiverNotifierImpl notifier = new FileArchiverNotifierImpl(conn, conf, fs, tn);
    List<SnapshotWithSize> snapshotsWithSizes = new ArrayList<>();
    try (Table table = conn.getTable(tn)) {
      // Writing no values will result in no records written.
      verify(table, () -> {
        notifier.persistSnapshotSizes(table, snapshotsWithSizes);
        assertEquals(0, count(table));
      });

      verify(table, () -> {
        snapshotsWithSizes.add(new SnapshotWithSize("ss1", 1024L));
        snapshotsWithSizes.add(new SnapshotWithSize("ss2", 4096L));
        notifier.persistSnapshotSizes(table, snapshotsWithSizes);
        assertEquals(2, count(table));
        assertEquals(1024L, extractSnapshotSize(table, tn, "ss1"));
        assertEquals(4096L, extractSnapshotSize(table, tn, "ss2"));
      });
    }
  }

  @Test
  public void testIncrementalFileArchiving() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(testName.getMethodName());
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }
    final Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME);
    final TableName tn1 = helper.createTableWithRegions(1);
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(
        tn1, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));

    // Write some data and flush it
    helper.writeData(tn1, 256L * SpaceQuotaHelperForTests.ONE_KILOBYTE);
    admin.flush(tn1);

    // Create a snapshot on the table
    final String snapshotName1 = tn1 + "snapshot1";
    admin.snapshot(new SnapshotDescription(snapshotName1, tn1, SnapshotType.SKIPFLUSH));

    FileArchiverNotifierImpl notifier = new FileArchiverNotifierImpl(conn, conf, fs, tn);
    long t1 = notifier.getLastFullCompute();
    long snapshotSize = notifier.computeAndStoreSnapshotSizes(Arrays.asList(snapshotName1));
    assertEquals("The size of the snapshots should be zero", 0, snapshotSize);
    assertTrue("Last compute time was not less than current compute time",
        t1 < notifier.getLastFullCompute());

    // No recently archived files and the snapshot should have no size
    Set<String> recentlyArchivedFiles = notifier.getRecentlyArchivedFiles();
    assertEquals("Files: " + recentlyArchivedFiles, 0, recentlyArchivedFiles.size());
    assertEquals(0, extractSnapshotSize(quotaTable, tn, snapshotName1));

    // Invoke the addArchivedFiles method with no files
    notifier.addArchivedFiles(Collections.emptyMap());

    // The size should not have changed
    recentlyArchivedFiles = notifier.getRecentlyArchivedFiles();
    assertEquals("Files: " + recentlyArchivedFiles, 0, recentlyArchivedFiles.size());
    assertEquals(0, extractSnapshotSize(quotaTable, tn, snapshotName1));

    notifier.addArchivedFiles(ImmutableMap.of("a", 1024L, "b", 1024L));

    // The size should not have changed
    recentlyArchivedFiles = notifier.getRecentlyArchivedFiles();
    assertEquals("Files: " + recentlyArchivedFiles, 0, recentlyArchivedFiles.size());
    assertEquals(0, extractSnapshotSize(quotaTable, tn, snapshotName1));

    // Pull one file referenced by the snapshot out of the manifest
    Set<String> referencedFiles = getFilesReferencedBySnapshot(snapshotName1);
    assertTrue("Found snapshot referenced files: " + referencedFiles, referencedFiles.size() >= 1);
    String referencedFile = Iterables.getFirst(referencedFiles, null);
    assertNotNull(referencedFile);

    // Report that a file this snapshot referenced was moved to the archive. This is a sign
    // that the snapshot should now "own" the size of this file
    final long fakeFileSize = 2048L;
    notifier.addArchivedFiles(ImmutableMap.of(referencedFile, fakeFileSize));

    // Verify that the snapshot owns this file.
    recentlyArchivedFiles = notifier.getRecentlyArchivedFiles();
    assertEquals("Files: " + recentlyArchivedFiles, 1, recentlyArchivedFiles.size());
    assertEquals(recentlyArchivedFiles, Collections.singleton(referencedFile));
    assertEquals(fakeFileSize, extractSnapshotSize(quotaTable, tn, snapshotName1));

    // In reality, we did not actually move the file, so a "full" computation should re-set the
    // size of the snapshot back to 0.
    long t2 = notifier.getLastFullCompute();
    snapshotSize = notifier.computeAndStoreSnapshotSizes(Arrays.asList(snapshotName1));
    assertEquals(0, snapshotSize);
    assertEquals(0, extractSnapshotSize(quotaTable, tn, snapshotName1));
    // We should also have no recently archived files after a re-computation
    recentlyArchivedFiles = notifier.getRecentlyArchivedFiles();
    assertEquals("Files: " + recentlyArchivedFiles, 0, recentlyArchivedFiles.size());
    assertTrue("Last compute time was not less than current compute time",
        t2 < notifier.getLastFullCompute());
  }

  @Test
  public void testParseOldNamespaceSnapshotSize() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName fakeQuotaTableName = TableName.valueOf(testName.getMethodName());
    final TableName tn = TableName.valueOf(testName.getMethodName() + "1");
    if (admin.tableExists(fakeQuotaTableName)) {
      admin.disableTable(fakeQuotaTableName);
      admin.deleteTable(fakeQuotaTableName);
    }
    admin.createTable(TableDescriptorBuilder.newBuilder(fakeQuotaTableName)
        .addFamily(new HColumnDescriptor(QuotaUtil.QUOTA_FAMILY_USAGE))
        .addFamily(new HColumnDescriptor(QuotaUtil.QUOTA_FAMILY_INFO))
        .build());

    final String ns = "";
    try (Table fakeQuotaTable = conn.getTable(fakeQuotaTableName)) {
      FileArchiverNotifierImpl notifier = new FileArchiverNotifierImpl(conn, conf, fs, tn);
      // Verify no record is treated as zero
      assertEquals(0, notifier.getPreviousNamespaceSnapshotSize(fakeQuotaTable, ns));

      // Set an explicit value of zero
      fakeQuotaTable.put(QuotaTableUtil.createPutNamespaceSnapshotSize(ns, 0L));
      assertEquals(0, notifier.getPreviousNamespaceSnapshotSize(fakeQuotaTable, ns));

      // Set a non-zero value
      fakeQuotaTable.put(QuotaTableUtil.createPutNamespaceSnapshotSize(ns, 1024L));
      assertEquals(1024L, notifier.getPreviousNamespaceSnapshotSize(fakeQuotaTable, ns));
    }
  }

  private long count(Table t) throws IOException {
    try (ResultScanner rs = t.getScanner(new Scan())) {
      long sum = 0;
      for (Result r : rs) {
        while (r.advance()) {
          sum++;
        }
      }
      return sum;
    }
  }

  private long extractSnapshotSize(
      Table quotaTable, TableName tn, String snapshot) throws IOException {
    Get g = QuotaTableUtil.makeGetForSnapshotSize(tn, snapshot);
    Result r = quotaTable.get(g);
    assertNotNull(r);
    CellScanner cs = r.cellScanner();
    assertTrue(cs.advance());
    Cell c = cs.current();
    assertNotNull(c);
    return QuotaTableUtil.extractSnapshotSize(
        c.getValueArray(), c.getValueOffset(), c.getValueLength());
  }

  private void verify(Table t, IOThrowingRunnable test) throws IOException {
    admin.disableTable(t.getName());
    admin.truncateTable(t.getName(), false);
    test.run();
  }

  @FunctionalInterface
  private interface IOThrowingRunnable {
    void run() throws IOException;
  }

  private Set<String> getFilesReferencedBySnapshot(String snapshotName) throws IOException {
    HashSet<String> files = new HashSet<>();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotName, FSUtils.getRootDir(conf));
    SnapshotProtos.SnapshotDescription sd = SnapshotDescriptionUtils.readSnapshotInfo(
        fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, sd);
    // For each region referenced by the snapshot
    for (SnapshotRegionManifest rm : manifest.getRegionManifests()) {
      // For each column family in this region
      for (FamilyFiles ff : rm.getFamilyFilesList()) {
        // And each store file in that family
        for (StoreFile sf : ff.getStoreFilesList()) {
          files.add(sf.getName());
        }
      }
    }
    return files;
  }
}
