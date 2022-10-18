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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * Physical hash join plan.
 */
public class PhysicalHashJoin<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends AbstractPhysicalJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    private boolean shouldTranslateOutput = true;

    private final List<Expression> filterConjuncts = Lists.newArrayList();

    public PhysicalHashJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, Optional.empty(), logicalProperties, leftChild,
                rightChild);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param hashJoinConjuncts conjunct list could use for build hash table in hash join
     */
    public PhysicalHashJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_HASH_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                groupExpression, logicalProperties, leftChild, rightChild);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param hashJoinConjuncts conjunct list could use for build hash table in hash join
     */
    public PhysicalHashJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_HASH_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                groupExpression, logicalProperties, physicalProperties, statsDeriveResult, leftChild, rightChild);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashJoin(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalHashJoin",
                "type", joinType,
                "hashJoinCondition", hashJoinConjuncts,
                "otherJoinCondition", otherJoinConjuncts,
                "stats", statsDeriveResult);
    }

    @Override
    public PhysicalHashJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                getLogicalProperties(), children.get(0), children.get(1));
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                groupExpression, getLogicalProperties(), left(), right());
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withLogicalProperties(
            Optional<LogicalProperties> logicalProperties) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                Optional.empty(), logicalProperties.get(), left(), right());
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                Optional.empty(), getLogicalProperties(), physicalProperties, statsDeriveResult, left(), right());
    }

    public boolean isShouldTranslateOutput() {
        return shouldTranslateOutput;
    }

    public void setShouldTranslateOutput(boolean shouldTranslateOutput) {
        this.shouldTranslateOutput = shouldTranslateOutput;
    }

    public List<Expression> getFilterConjuncts() {
        return filterConjuncts;
    }
}
