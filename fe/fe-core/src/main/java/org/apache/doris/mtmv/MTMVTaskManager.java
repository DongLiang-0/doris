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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.mtmv.MTMVUtils.JobState;
import org.apache.doris.mtmv.MTMVUtils.TaskState;
import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MTMVTaskManager {

    private static final Logger LOG = LogManager.getLogger(MTMVTaskManager.class);

    // jobId -> pending tasks, one job can dispatch many tasks
    private final Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> pendingTaskMap = Maps.newConcurrentMap();

    // jobId -> running tasks, only one task will be running for one job.
    private final Map<Long, MTMVTaskExecutor> runningTaskMap = Maps.newConcurrentMap();

    private final MTMVTaskExecutorPool taskExecutorPool = new MTMVTaskExecutorPool();

    private final ReentrantLock reentrantLock = new ReentrantLock(true);

    // keep track of all the completed tasks
    private final Deque<MTMVTask> historyTasks = Queues.newLinkedBlockingDeque();

    private ScheduledExecutorService taskScheduler = Executors.newScheduledThreadPool(1);

    private final MTMVJobManager mtmvJobManager;

    private final AtomicInteger failedTaskCount = new AtomicInteger(0);

    public MTMVTaskManager(MTMVJobManager mtmvJobManager) {
        this.mtmvJobManager = mtmvJobManager;
    }

    public void startTaskScheduler() {
        if (taskScheduler.isShutdown()) {
            taskScheduler = Executors.newScheduledThreadPool(1);
        }
        taskScheduler.scheduleAtFixedRate(() -> {
            if (!tryLock()) {
                return;
            }
            try {
                checkRunningTask();
                scheduledPendingTask();
            } catch (Exception ex) {
                LOG.warn("failed to schedule task.", ex);
            } finally {
                unlock();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void stopTaskScheduler() {
        taskScheduler.shutdown();
    }

    public MTMVUtils.TaskSubmitStatus submitTask(MTMVTaskExecutor taskExecutor, MTMVTaskExecuteParams params) {
        // duplicate submit
        if (taskExecutor.getTask() != null) {
            return MTMVUtils.TaskSubmitStatus.FAILED;
        }

        int validPendingCount = 0;
        for (Long jobId : pendingTaskMap.keySet()) {
            if (!pendingTaskMap.get(jobId).isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= Config.max_pending_mtmv_scheduler_task_num) {
            LOG.warn("pending task exceeds pending_scheduler_task_size:{}, reject the submit.",
                    Config.max_pending_mtmv_scheduler_task_num);
            return MTMVUtils.TaskSubmitStatus.REJECTED;
        }

        String taskId = UUID.randomUUID().toString();
        MTMVTask task = taskExecutor.initTask(taskId, MTMVUtils.getNowTimeStamp());
        task.setPriority(params.getPriority());
        LOG.info("Submit a mtmv task with id: {} of the job {}.", taskId, taskExecutor.getJob().getName());
        arrangeToPendingTask(taskExecutor);
        return MTMVUtils.TaskSubmitStatus.SUBMITTED;
    }

    public boolean killTask(Long jobId, boolean clearPending) {
        if (clearPending) {
            if (!tryLock()) {
                return false;
            }
            try {
                getPendingTaskMap().remove(jobId);
            } catch (Exception ex) {
                LOG.warn("failed to kill task.", ex);
            } finally {
                unlock();
            }
        }
        MTMVTaskExecutor task = runningTaskMap.get(jobId);
        if (task == null) {
            return false;
        }
        ConnectContext connectContext = task.getCtx();
        if (connectContext != null) {
            connectContext.kill(false);
            return true;
        }
        return false;
    }

    public void arrangeToPendingTask(MTMVTaskExecutor task) {
        if (!tryLock()) {
            return;
        }
        try {
            long jobId = task.getJobId();
            PriorityBlockingQueue<MTMVTaskExecutor> tasks =
                    pendingTaskMap.computeIfAbsent(jobId, u -> Queues.newPriorityBlockingQueue());
            tasks.offer(task);
        } finally {
            unlock();
        }
    }

    @Nullable
    private MTMVTaskExecutor getTask(PriorityBlockingQueue<MTMVTaskExecutor> tasks, MTMVTaskExecutor task) {
        MTMVTaskExecutor oldTask = null;
        for (MTMVTaskExecutor t : tasks) {
            if (t.equals(task)) {
                oldTask = t;
                break;
            }
        }
        return oldTask;
    }

    private void checkRunningTask() {
        Iterator<Long> runningIterator = runningTaskMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long jobId = runningIterator.next();
            MTMVTaskExecutor taskExecutor = runningTaskMap.get(jobId);
            if (taskExecutor == null) {
                LOG.warn("failed to get running task by jobId:{}", jobId);
                runningIterator.remove();
                return;
            }
            Future<?> future = taskExecutor.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                addHistory(taskExecutor.getTask());
                MTMVUtils.TaskState finalState = taskExecutor.getTask().getState();
                if (finalState == TaskState.FAILURE) {
                    failedTaskCount.incrementAndGet();
                }
                Env.getCurrentEnv().getEditLog().logCreateMTMVTask(taskExecutor.getTask());

                TriggerMode triggerMode = taskExecutor.getJob().getTriggerMode();
                if (triggerMode == TriggerMode.ONCE) {
                    // update the run once job status
                    ChangeMTMVJob changeJob = new ChangeMTMVJob(taskExecutor.getJobId(), JobState.COMPLETE);
                    mtmvJobManager.updateJob(changeJob, false);
                } else if (triggerMode == TriggerMode.PERIODICAL) {
                    // just update the last modify time.
                    ChangeMTMVJob changeJob = new ChangeMTMVJob(taskExecutor.getJobId(), JobState.ACTIVE);
                    mtmvJobManager.updateJob(changeJob, false);
                }
            }
        }
    }

    public int getFailedTaskCount() {
        return failedTaskCount.get();
    }

    private void scheduledPendingTask() {
        int currentRunning = runningTaskMap.size();

        Iterator<Long> pendingIterator = pendingTaskMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long jobId = pendingIterator.next();
            MTMVTaskExecutor runningTaskExecutor = runningTaskMap.get(jobId);
            if (runningTaskExecutor == null) {
                Queue<MTMVTaskExecutor> taskQueue = pendingTaskMap.get(jobId);
                if (taskQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= Config.max_running_mtmv_scheduler_task_num) {
                        break;
                    }
                    MTMVTaskExecutor pendingTaskExecutor = taskQueue.poll();
                    taskExecutorPool.executeTask(pendingTaskExecutor);
                    runningTaskMap.put(jobId, pendingTaskExecutor);
                    currentRunning++;
                }
            }
        }
    }

    public boolean tryLock() {
        try {
            return reentrantLock.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task lock", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }


    public void unlock() {
        this.reentrantLock.unlock();
    }

    public Map<Long, PriorityBlockingQueue<MTMVTaskExecutor>> getPendingTaskMap() {
        return pendingTaskMap;
    }

    public Map<Long, MTMVTaskExecutor> getRunningTaskMap() {
        return runningTaskMap;
    }

    private void addHistory(MTMVTask task) {
        historyTasks.addFirst(task);
    }

    public Deque<MTMVTask> getHistoryTasks() {
        return historyTasks;
    }

    public List<MTMVTask> showAllTasks() {
        return showTasks(null);
    }

    public List<MTMVTask> showTasks(String dbName) {
        List<MTMVTask> taskList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(dbName)) {
            for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(pTaskQueue.stream().map(MTMVTaskExecutor::getTask).collect(Collectors.toList()));
            }
            taskList.addAll(
                    getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask).collect(Collectors.toList()));
            taskList.addAll(getHistoryTasks());
        } else {
            for (Queue<MTMVTaskExecutor> pTaskQueue : getPendingTaskMap().values()) {
                taskList.addAll(
                        pTaskQueue.stream().map(MTMVTaskExecutor::getTask).filter(u -> u.getDBName().equals(dbName))
                                .collect(Collectors.toList()));
            }
            taskList.addAll(getRunningTaskMap().values().stream().map(MTMVTaskExecutor::getTask)
                    .filter(u -> u.getDBName().equals(dbName)).collect(Collectors.toList()));
            taskList.addAll(
                    getHistoryTasks().stream().filter(u -> u.getDBName().equals(dbName)).collect(Collectors.toList()));

        }
        return taskList.stream().sorted().collect(Collectors.toList());
    }

    public List<MTMVTask> showTasks(String dbName, String mvName) {
        return showTasks(dbName).stream().filter(u -> u.getMVName().equals(mvName)).collect(Collectors.toList());
    }

    public MTMVTask getTask(String taskId) throws AnalysisException {
        List<MTMVTask> tasks =
                showAllTasks().stream().filter(u -> u.getTaskId().equals(taskId)).collect(Collectors.toList());
        if (tasks.size() == 0) {
            throw new AnalysisException("Can't find the task id in the task list.");
        } else if (tasks.size() > 1) {
            throw new AnalysisException("Find more than one task id in the task list.");
        } else {
            return tasks.get(0);
        }
    }

    public void replayCreateJobTask(MTMVTask task) {
        addHistory(task);
    }

    public void clearTasksByJobName(String jobName, boolean isReplay) {
        List<String> clearTasks = Lists.newArrayList();

        if (!tryLock()) {
            return;
        }
        try {
            List<MTMVTask> taskHistory = showAllTasks();
            for (MTMVTask task : taskHistory) {
                if (task.getJobName().equals(jobName)) {
                    clearTasks.add(task.getTaskId());
                }
            }
        } finally {
            unlock();
        }
        dropTasks(clearTasks, isReplay);
    }

    public void removeExpiredTasks() {
        long currentTime = MTMVUtils.getNowTimeStamp();

        List<String> historyToDelete = Lists.newArrayList();

        if (!tryLock()) {
            return;
        }
        try {
            Deque<MTMVTask> taskHistory = getHistoryTasks();
            for (MTMVTask task : taskHistory) {
                long expireTime = task.getExpireTime();
                if (currentTime > expireTime) {
                    historyToDelete.add(task.getTaskId());
                }
            }
        } finally {
            unlock();
        }
        dropTasks(historyToDelete, false);
    }

    public void dropTasks(List<String> taskIds, boolean isReplay) {
        if (taskIds.isEmpty()) {
            return;
        }
        if (!tryLock()) {
            return;
        }
        try {
            Set<String> taskSet = new HashSet<>(taskIds);
            // Pending tasks will be clear directly. So we don't drop it again here.
            // Check the running task since the task was killed but was not move to the history queue.
            if (!isReplay) {
                for (long key : runningTaskMap.keySet()) {
                    MTMVTaskExecutor executor = runningTaskMap.get(key);
                    // runningTaskMap may be removed in the runningIterator
                    if (executor != null && taskSet.contains(executor.getTask().getTaskId())) {
                        runningTaskMap.remove(key);
                    }
                }
            }
            // Try to remove history tasks.
            getHistoryTasks().removeIf(mtmvTask -> taskSet.contains(mtmvTask.getTaskId()));
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logDropMTMVTasks(taskIds);
            }
        } finally {
            unlock();
        }
        LOG.info("drop task history:{}", taskIds);
    }
}
