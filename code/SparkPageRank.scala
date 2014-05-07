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

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 */
object SparkPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: PageRank <master> <input_dir> <number_of_iterations> <number_of_top_pr_print>")
      System.exit(1)
    }
    var iters = args(2).toInt
    val ctx = new SparkContext(args(0), "PageRank",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val lines = ctx.textFile(args(1), 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    
    val output = ranks.map {
        case (key, value) => (value, key);
    }.sortByKey(false).top(args(3).toInt)
    output.foreach(tup => println(tup._2 + " has rank: " + tup._1 + "."))

    ctx.stop()
  }
}
