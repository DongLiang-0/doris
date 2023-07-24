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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.properties.DistributionSpecAny;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEsScan;

import java.util.Optional;

/**
 * Implementation rule that convert logical JdbcScan to physical JdbcScan.
 */
public class LogicalEsScanToPhysicalEsScan extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalEsScan().then(esScan ->
            new PhysicalEsScan(
                esScan.getRelationId(),
                esScan.getTable(),
                esScan.getQualifier(),
                DistributionSpecAny.INSTANCE,
                Optional.empty(),
                esScan.getLogicalProperties())
        ).toRule(RuleType.LOGICAL_ES_SCAN_TO_PHYSICAL_ES_SCAN_RULE);
    }
}
