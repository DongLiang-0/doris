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

suite("test_hive_statistic_cache", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic_cache"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use statistics;"""
        qt_1 "select count(*) from stats"
        Thread.sleep(5000);
        qt_2 "show column cached stats `stats` (lo_orderkey)"
        qt_3 "show column cached stats `stats` (lo_linenumber)"
        qt_4 "show column cached stats `stats` (lo_custkey)"
        qt_5 "show column cached stats `stats` (lo_partkey)"
        qt_6 "show column cached stats `stats` (lo_suppkey)"
        qt_7 "show column cached stats `stats` (lo_orderdate)"
        qt_8 "show column cached stats `stats` (lo_orderpriority)"
        qt_9 "show column cached stats `stats` (lo_shippriority)"
        qt_10 "show column cached stats `stats` (lo_extendedprice)"
        qt_11 "show column cached stats `stats` (lo_ordtotalprice)"
        qt_12 "show column cached stats `stats` (lo_discount)"
        qt_13 "show column cached stats `stats` (lo_revenue)"
        qt_14 "show column cached stats `stats` (lo_supplycost)"
        qt_15 "show column cached stats `stats` (lo_tax)"
        qt_16 "show column cached stats `stats` (lo_commitdate)"
        qt_17 "show column cached stats `stats` (lo_shipmode)"
        qt_18 "show column cached stats `stats` (lo_orderkey)"
        qt_19 "show column cached stats `stats` (lo_quantity)"
    }
}

