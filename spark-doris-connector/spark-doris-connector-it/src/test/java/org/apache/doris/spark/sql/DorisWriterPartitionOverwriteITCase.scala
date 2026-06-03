// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.spark.sql

import org.apache.doris.spark.container.AbstractContainerTestBase.{assertEqualsInAnyOrder, getDorisQueryConnection}
import org.apache.doris.spark.container.{AbstractContainerTestBase, ContainerUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * IT case for Doris Writer Partition Overwrite functionality.
 */
class DorisWriterPartitionOverwriteITCase extends AbstractContainerTestBase {

  private val LOG = LoggerFactory.getLogger(classOf[DorisWriterPartitionOverwriteITCase])

  val DATABASE: String = "test_doris_write"
  val TABLE_PARTITION_OVERWRITE: String = "test_partition_overwrite"


  /**
   * Test 1: Overwrite single partition
   */
  @Test
  @throws[Exception]
  def testOverwriteSinglePartition(): Unit = {
    initializePartitionTable()
    
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02"),
        (3, "Charlie", 35, "2024-01-03")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Overwrite only partition p20240101
      val df2 = session.createDataFrame(Seq(
        (10, "Alice_new", 26, "2024-01-01"),
        (11, "David", 40, "2024-01-01")
      )).toDF("id", "name", "age", "dt")

      df2.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .option("doris.write.overwrite.partitions", "p20240101")
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify: partition p20240101 is overwritten, other partitions remain unchanged
      val actualAll = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s order by id", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList(
        "2,Bob",
        "3,Charlie",
        "10,Alice_new",
        "11,David"
      )
      checkResultInAnyOrder("testOverwriteSinglePartition", expected.toArray, actualAll.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 2: Overwrite multiple partitions
   */
  @Test
  @throws[Exception]
  def testOverwriteMultiplePartitions(): Unit = {
    initializePartitionTable()
    
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02"),
        (3, "Charlie", 35, "2024-01-03")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Overwrite partitions p20240101 and p20240102
      val df2 = session.createDataFrame(Seq(
        (10, "Alice_v2", 26, "2024-01-01"),
        (20, "Bob_v2", 31, "2024-01-02"),
        (21, "Eve", 28, "2024-01-02")
      )).toDF("id", "name", "age", "dt")

      df2.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .option("doris.write.overwrite.partitions", "p20240101,p20240102")
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify: p20240101 and p20240102 are overwritten, p20240103 remains unchanged
      val actualAll = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s order by id", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList(
        "3,Charlie",
        "10,Alice_v2",
        "20,Bob_v2",
        "21,Eve"
      )
      checkResultInAnyOrder("testOverwriteMultiplePartitions", expected.toArray, actualAll.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 3: Overwrite without partitions specified (fallback to full table overwrite)
   */
  @Test
  @throws[Exception]
  def testOverwriteWithoutPartitionsFallbackToFullTable(): Unit = {
    initializePartitionTable()

    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // No partition parameter specified, should do full table overwrite
      val df2 = session.createDataFrame(Seq(
        (100, "NewData", 99, "2024-01-01")
      )).toDF("id", "name", "age", "dt")

      df2.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify: Full table is overwritten
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList("100,NewData")
      checkResultInAnyOrder("testOverwriteWithoutPartitionsFallbackToFullTable", expected.toArray, actual.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 4: Overwrite with empty partition list (fallback to full table)
   */
  @Test
  @throws[Exception]
  def testOverwriteWithEmptyPartitionList(): Unit = {
    initializePartitionTable()

    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Empty partition list, should fallback to full table overwrite
      val df2 = session.createDataFrame(Seq(
        (100, "EmptyListData", 88, "2024-01-01")
      )).toDF("id", "name", "age", "dt")

      df2.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .option("doris.write.overwrite.partitions", "")
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify: Full table is overwritten
      val actual = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList("100,EmptyListData")
      checkResultInAnyOrder("testOverwriteWithEmptyPartitionList", expected.toArray, actual.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 5: Overwrite with Spark SQL syntax
   */
  @Test
  @throws[Exception]
  def testOverwriteWithSparkSQL(): Unit = {
    initializePartitionTable()
    
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-01"),
        (3, "Charlie", 35, "2024-01-02")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Use Spark SQL to create temporary view and execute overwrite
      val df2 = session.createDataFrame(Seq(
        (10, "SQL_Alice", 26, "2024-01-01"),
        (11, "SQL_David", 40, "2024-01-01")
      )).toDF("id", "name", "age", "dt")
      df2.createTempView("new_partition_data")

      session.sql(
        s"""
           |CREATE TEMPORARY VIEW doris_partition_table
           |USING doris
           |OPTIONS(
           | "table.identifier"="${DATABASE + "." + TABLE_PARTITION_OVERWRITE}",
           | "fenodes"="${getFenodes}",
           | "user"="${getDorisUsername}",
           | "password"="${getDorisPassword}",
           | "doris.query.port"="${getQueryPort}",
           | "doris.write.overwrite.partitions"="p20240101"
           |)
           |""".stripMargin)

      session.sql(
        """
          |insert overwrite table doris_partition_table 
          |select id, name, age, dt from new_partition_data
          |""".stripMargin)

      Thread.sleep(10000)
      
      // Verify: p20240101 is overwritten, p20240102 remains unchanged
      val actualAll = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s order by id", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList(
        "3,Charlie",
        "10,SQL_Alice",
        "11,SQL_David"
      )
      checkResultInAnyOrder("testOverwriteWithSparkSQL", expected.toArray, actualAll.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 6: Overwrite partition with data that spans multiple partitions
   */
  @Test
  @throws[Exception]
  def testOverwriteWithDataSpanningMultiplePartitions(): Unit = {
    initializePartitionTable()
    
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02"),
        (3, "Charlie", 35, "2024-01-03")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Overwrite only p20240101, but data spans p20240101 and p20240102
      val df2 = session.createDataFrame(Seq(
        (10, "New_Alice", 26, "2024-01-01"),   // p20240101
        (11, "New_Bob", 31, "2024-01-02"),     // p20240102
        (12, "New_Eve", 28, "2024-01-02")       // p20240102
      )).toDF("id", "name", "age", "dt")

      df2.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .option("doris.write.overwrite.partitions", "p20240101")
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify:
      // - p20240101 is truncated and New_Alice is written
      // - p20240102 retains original Bob and adds New_Bob, New_Eve
      // - p20240103 remains unchanged
      val actualAll = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s order by id", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val expected = util.Arrays.asList(
        "2,Bob",
        "3,Charlie",
        "10,New_Alice",
        "11,New_Bob",
        "12,New_Eve"
      )
      checkResultInAnyOrder("testOverwriteWithDataSpanningMultiplePartitions", expected.toArray, actualAll.toArray)
      
    } finally {
      session.stop()
    }
  }

  /**
   * Test 7: Overwrite partition with empty data (truncate only)
   */
  @Test
  @throws[Exception]
  def testOverwritePartitionWithEmptyData(): Unit = {
    initializePartitionTable()
    
    val session = SparkSession.builder().master("local[1]").getOrCreate()
    try {
      // Write initial data first
      val df1 = session.createDataFrame(Seq(
        (1, "Alice", 25, "2024-01-01"),
        (2, "Bob", 30, "2024-01-02")
      )).toDF("id", "name", "age", "dt")

      df1.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .mode(SaveMode.Append)
        .save()

      Thread.sleep(10000)
      
      // Write empty data, truncate only
      import session.implicits._
      val emptyDF = Seq.empty[(Int, String, Int, String)].toDF("id", "name", "age", "dt")

      emptyDF.write
        .format("doris")
        .option("doris.fenodes", getFenodes)
        .option("doris.table.identifier", DATABASE + "." + TABLE_PARTITION_OVERWRITE)
        .option("user", getDorisUsername)
        .option("password", getDorisPassword)
        .option("doris.query.port", getQueryPort)
        .option("doris.write.overwrite.partitions", "p20240101")
        .mode(SaveMode.Overwrite)
        .save()

      Thread.sleep(10000)
      
      // Verify: p20240101 is empty, p20240102 remains unchanged
      val actualP1 = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select * from %s.%s partition(p20240101)", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      val actualP2 = ContainerUtils.executeSQLStatement(
        getDorisQueryConnection,
        LOG,
        String.format("select id,name from %s.%s partition(p20240102)", DATABASE, TABLE_PARTITION_OVERWRITE),
        2
      )
      
      assert(actualP1.isEmpty)
      val expectedP2 = util.Arrays.asList("2,Bob")
      checkResultInAnyOrder("testOverwritePartitionWithEmptyData", expectedP2.toArray, actualP2.toArray)
      
    } finally {
      session.stop()
    }
  }

  private def initializePartitionTable(): Unit = {
    val targetInitSql: Array[String] = ContainerUtils.parseFileContentSQL("container/ddl/write_partition_overwrite.sql")
    ContainerUtils.executeSQLStatement(getDorisQueryConnection, LOG, Array(String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE)): _*)
    val connection = getDorisQueryConnection(DATABASE)
    ContainerUtils.executeSQLStatement(connection, LOG, targetInitSql: _*)
  }

  private def checkResultInAnyOrder(testName: String, expected: Array[AnyRef], actual: Array[AnyRef]): Unit = {
    LOG.info("Checking DorisWriterPartitionOverwriteITCase result. testName={}, actual={}, expected={}", testName, actual, expected)
    assertEqualsInAnyOrder(expected.toList.asJava, actual.toList.asJava)
  }
}