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

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext


object SparkError {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SparkError <master> <input> <output>")
      System.exit(1)
    }
    val ctx = new SparkContext(args(0), "SparkError",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val lines = ctx.textFile(args(1), 1)
    val format = lines.filter(
      _.split("\\s+").length != 2
    )
	
    format.saveAsTextFile(args(2))
    System.exit(0)
  }
}

