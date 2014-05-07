/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark._

object SparkGraphCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkGraphCount <master> <input>")
      System.exit(1)
    }
    val inputPath = args(1)
    val sc = new SparkContext(args(0), "SparkGraphCount",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val lines = sc.textFile(inputPath)

    val numEdge = lines.count()
    val numVertex = lines
      .flatMap(line => line.split("\\s+"))
      .distinct.count()

    println(("the number of vertex is %d, " +
      "the number of edge is %d").format(numVertex, numEdge))

    System.exit(0)
  }
}

