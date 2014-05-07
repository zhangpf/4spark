#!/bin/bash
# Copyright (C) 2014 Pengfei Zhang
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# This file is used to transfer the spark-prject needed files in master node 
# to each slave nodes. We use the rsync tools to keep track of files list.
# Usage:
#	1. modify the ${spark_dir} to the path of spark-project.
#	2. modify the spark_jar_path if the {scala, hadoop, spark} version 
#	   has been changed.
#

spark_dir="/home/hadoop/spark"
spark_conf_dir=${spark_dir}"/conf"
spark_jar_path=${spark_dir}"/assembly/target/scala-2.10/spark-assembly-1.0.0-SNAPSHOT-hadoop1.0.4.jar"
spark_sbin_dir=${spark_dir}"/sbin"
spark_bin_dir=${spark_dir}"/bin"

for host in `cat ${spark_dir}/conf/slaves | cut -f2 -d ' '`
do
	if  [ -n $host ] && [ $host != `hostname` ]; then
		rsync -avlRr --delete ${spark_conf_dir} ${host}:/
		rsync -avlRr --delete ${spark_sbin_dir} ${host}:/
		rsync -avlRr --delete ${spark_bin_dir} ${host}:/
		rsync -avlRr --delete ${spark_jar_path} ${host}:/
        fi
done
