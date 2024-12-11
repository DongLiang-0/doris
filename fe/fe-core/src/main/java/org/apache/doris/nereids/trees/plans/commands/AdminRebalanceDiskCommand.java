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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * admin rebalance disk
 */
public class AdminRebalanceDiskCommand extends AbstractRebalanceDiskCommand {
    private static final Logger LOG = LogManager.getLogger(AdminRebalanceDiskCommand.class);
    private final long timeoutS = 24 * 3600; // default 24 hours
    private List<String> backends;

    public AdminRebalanceDiskCommand() {
        super(PlanType.ADMIN_REBALANCE_DISK_COMMAND);
    }

    public AdminRebalanceDiskCommand(List<String> backends) {
        super(PlanType.ADMIN_REBALANCE_DISK_COMMAND);
        this.backends = backends;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        handleRebalanceDisk();
    }

    private void handleRebalanceDisk() throws AnalysisException {
        List<Backend> rebalanceDiskBackends = getNeedRebalanceDiskBackends(backends);
        if (rebalanceDiskBackends.isEmpty()) {
            LOG.info("The matching be is empty, no be to rebalance disk.");
            return;
        }
        Env.getCurrentEnv().getTabletScheduler().rebalanceDisk(rebalanceDiskBackends, timeoutS);
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminRebalanceDiskCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminRebalanceDiskCommand(this, context);
    }
}
