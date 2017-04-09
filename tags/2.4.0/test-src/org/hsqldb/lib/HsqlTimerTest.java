/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.hsqldb.lib;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(org.hsqldb.lib.HsqlTimer.class)
public class HsqlTimerTest extends BaseTestCase {

    public static final String SUBJECT_CLASS_NAME = org.hsqldb.lib.HsqlTimer.class.getName();
    //
    public static final String PK_IS_TEST = "test." + SUBJECT_CLASS_NAME;
    //
    public static final String PK_AVG_SYNC_TIME_ATTEMPTED_SYNCS = "test." + SUBJECT_CLASS_NAME + ".avgSyncTime.attemptedSyncs";
    public static final String PK_AVG_SYNC_TIME_MAX_THREADS = "test." + SUBJECT_CLASS_NAME + ".avgSyncTime.maxThreads";
    public static final String PK_AVG_SYNC_TIME_BUFFSIZE = "test." + SUBJECT_CLASS_NAME + ".avgSyncTime.buffSize";
    //
    public static final String PK_TASK_COUNT = "test." + SUBJECT_CLASS_NAME + ".taskCount";
    public static final String PK_SCHEDULING_PERIOD_MULTIPLIER = "test." + SUBJECT_CLASS_NAME + ".schedulingPeriodMultiplier";
    public static final String PK_TEST_DURATION = "test." + SUBJECT_CLASS_NAME + ".testDuration";
    //
    public static final String TMP_FILE_NAME_PREFIX = "HsqlTimerTest_AvgSyncTime";
    public static final String TMP_FILE_NAME_EXT = ".tmp";
    //
    public static final String RAF_RW_MODE = "rw";

    /**
     * Computes the system average {@link java.io.FileDescriptor#sync()
     * sync} time.
     *
     * @param attemptedSyncs # of sync operations to attempt when computing the average
     * @param maxThreads to run in parallel to perform the sync operations
     * @param buffSize the size of data buffer to write before each sync call
     * @return the total time to write buff and call sync runs times, divided by
     * runs
     */
    double avgSyncTime(final int attemptedSyncs, final int maxThreads, final int buffSize) throws Exception {

        final File file = File.createTempFile(TMP_FILE_NAME_PREFIX, TMP_FILE_NAME_EXT);
        final RandomAccessFile raf = new RandomAccessFile(file, RAF_RW_MODE);
        final FileDescriptor fd = raf.getFD();
        final int threadCount = Math.min(maxThreads, attemptedSyncs);
        final int attemptedSyncsPerThread = Math.max(1, attemptedSyncs / threadCount);
        final int[] actualSyncsCompleted = new int[1];
        final byte[] buff = new byte[buffSize];

        Runnable task = new Runnable() {

            public void run() {
                for (int i = 0; i < attemptedSyncsPerThread; i++) {
                    try {
                        raf.seek(0);
                        raf.write(buff);
                        fd.sync();
                        actualSyncsCompleted[0] = actualSyncsCompleted[0] + 1;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        };

        Thread[] tasks = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            tasks[i] = new Thread(task);
        }

        println("Derived parameters:");
        println("------------------------------------------.");
        println("threadCount (theoretical parallel degree) : " + threadCount);
        println("attemptedSyncsPerThread                   : " + attemptedSyncsPerThread);
        println("------------------------------------------.");
        println("Working.  Please wait...");
        println("Serial execution test");
        double start = System.currentTimeMillis();
        for (int i = 0; i < attemptedSyncs; i++) {
            try {
                raf.seek(0);
                raf.write(buff);
                fd.sync();
                actualSyncsCompleted[0] = actualSyncsCompleted[0] + 1;
            } catch (Exception ex) {
            }
        }
        double elapsed = System.currentTimeMillis() - start;
        double totalSerialExecutionTime = elapsed;
        int    totalSerialExectionSyncs = actualSyncsCompleted[0];
        double avgTimeperSerialSync = totalSerialExecutionTime / totalSerialExectionSyncs;
        println("Done.");
        println("------------------------------------.");
        println("Time elapsed                        : " + totalSerialExecutionTime + " millis");
        println("Sync invocations actually completed : " + totalSerialExectionSyncs);
        println("Avgerage time per sync              : " + avgTimeperSerialSync + " millis");
        println("------------------------------------.");
        println("Parallel execution test");
        actualSyncsCompleted[0] = 0;
        start = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            tasks[i].start();
        }

        for (int i = 0; i < threadCount; i++) {
            try {
                tasks[i].join();
            } catch (InterruptedException ex) {
            }
        }

        elapsed = System.currentTimeMillis() - start;
        double totalParallelExecutionTime = elapsed;
        int totalParallelExecutionSyncs = actualSyncsCompleted[0];
        double avgTimePerParallelSync = totalParallelExecutionTime / totalParallelExecutionSyncs;

        println("Done.");
        println("------------------------------------.");
        println("Time elapsed                        : " + totalParallelExecutionTime + " millis");
        println("Sync invocations actually completed : " + totalParallelExecutionSyncs);
        println("Average time per sync               : " + avgTimePerParallelSync + " millis");
        println("------------------------------------.");
        println("Parallel sync coalescing factor     : " + (totalParallelExecutionTime/totalSerialExecutionTime));
        println("------------------------------------.");

        double actualParallelDegree = threadCount * (totalParallelExecutionSyncs / (double) attemptedSyncs);

        println("Actual parallel degree (thread count * (actual syncs / attempted syncs): " + actualParallelDegree);

        try {
            raf.close();
        } catch (IOException ex) {
        }
        try {
            file.delete();
        } catch (Exception e) {
        }

       return avgTimePerParallelSync;
    }

    /**
     * WRITE_DELAY simulation task.
     *
     * Writes a given buffer to disk, sync's the associated file descriptor and
     * maintains an account of the average period between executions.
     */
    static class WriteAndSyncTask extends java.util.TimerTask {
        // static

        /**
         * Used to make the name of each task unique.
         */
        static int serial;
        /**
         * The data to write.
         */
        static final byte[] buf = new byte[256];
        // instance
        /**
         * Identifies this task.
         */
        String name;
        /**
         * The time at which this task was last executed.
         */
        long last;
        /**
         * A running sum of the periods between executions.
         */
        long total;
        /**
         * The number of times this task has been executed.
         */
        int runs;
        /**
         * True until this task is the first time.
         */
        boolean firstTime = true;
        /**
         * The file to write.
         */
        java.io.File file;
        /**
         * The FileOutputStream to write.
         */
        java.io.FileOutputStream fos;
        /**
         * The FileDescriptor to sync.
         */
        java.io.FileDescriptor fd;

        /**
         * Constructs a new WriteAndSyncTask
         */
        WriteAndSyncTask() {
            this.name = "Task." + serial++;

            try {
                this.file = java.io.File.createTempFile(name, TMP_FILE_NAME_EXT);
                this.fos = new java.io.FileOutputStream(file);
                this.fd = fos.getFD();
            } catch (java.io.IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        /**
         * Runnable implementation. <p>
         *
         * Does the average period accounting and invokes the writeAndSync
         * method.
         */
        @Override
        public void run() {
            final long now = System.currentTimeMillis();

            if (this.firstTime) {
                this.firstTime = false;
            } else {
                this.total += (now - this.last);
            }

            this.last = now;

            writeAndSync();

            this.runs++;
        }

        /**
         * Write a given buffer to disk and sync the associated file
         * descriptor.
         */
        @SuppressWarnings("CallToThreadDumpStack")
        void writeAndSync() {
            try {
                this.fos.write(buf);
                this.fos.flush();
                this.fd.sync();
                //Thread.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Closes the FileOutputStream, deletes the file and nullifies Object
         * fields.
         */
        @SuppressWarnings("CallToThreadDumpStack")
        public void release() {
            try {
                this.fos.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                this.file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.fos = null;
            this.file = null;
            this.fd = null;
        }

        /**
         * Retrieves the computed moment of actual average periodicity
         * experienced by this task.
         */
        public float getAveragePeriod() {
            return (this.runs < 2) ? Float.NaN
                    : (this.total / (float) (this.runs - 1));
        }

        /**
         * @return the String representation of this task, indicating its name,
         * the number of runs so far and the computed moment of actual average
         * periodicity experienced so far.
         */
        @Override
        public String toString() {
            return this.name
                    + "["
                    + "runs: " + runs + ", "
                    + "actual avg. period: " + getAveragePeriod()
                    + "]";
        }
    }

    static class Stats {

        double min;
        double max;
        double pk;
        double sk;
        double vk;
        long n;
        boolean initialized;
        boolean sample;

        void addDataPoint(double x) {

            double xi;
            double xsi;
            long nm1;

            xi = x;

            if (!initialized) {
                n = 1;
                pk = xi;
                sk = xi;
                min = xi;
                max = xi;
                vk = 0.0;
                initialized = true;

                return;
            }

            n++;

            nm1 = (n - 1);
            xsi = (sk - (xi * nm1));
            vk += ((xsi * xsi) / n) / nm1;
            sk += xi;

            if (xi != 0) {
                pk *= xi;
            }

            max = Math.max(max, xi);
            min = Math.min(min, xi);
        }

        double getMin() {
            return initialized ? min : Double.NaN;
        }

        double getMax() {
            return initialized ? max : Double.NaN;
        }

        double getGeometricMean() {
            return initialized ? Math.pow(pk, 1 / (double) n) : Double.NaN;
        }

        double getVariance() {

            if (!initialized) {
                return Double.NaN;
            }

            return sample ? (n == 1) ? Double.NaN
                    : (vk / (double) (n - 1))
                    : (vk / (double) (n));
        }

        double getStdDev() {

            if (!initialized) {
                return Double.NaN;
            }

            return sample ? (n == 1) ? Double.NaN
                    : (Math.sqrt(vk
                    / (double) (n - 1)))
                    : (Math.sqrt(vk / (double) (n)));
        }
    }

    /**
     * Runs the HsqlTimer tests.
     *
     * @param args Currently unused
     */
    @OfMethod("<<ALL_METHODS>>")
    public void testHsqlTimer() throws Exception {
        if (!getBooleanProperty(PK_IS_TEST, true)) {
            println("DISABLED: " + PK_IS_TEST);
            return;
        }
        // number of tasks to queue
        int taskCount = getIntProperty(PK_TASK_COUNT, 50);
        // period, as a multiple of computed system-specific avg. sync time
        double taskPeriodMultiplier = getDoubleProperty(PK_SCHEDULING_PERIOD_MULTIPLIER, 1.0D);
        // how long to run the timer, in milliseconds
        long testDuration = getIntProperty(PK_TEST_DURATION, 5000); // millis

        test(taskCount, taskPeriodMultiplier, testDuration);
    }

    /**
     * Runs the HsqlTimer and java.util.Timer tests using the given arguments.
     * <p>
     *
     * @param taskCount the number of WriteAndSync tasks to run for the
     * requested duration of the test
     * @param taskPeriodMultiplier the period with with to schedule the tasks,
     * as a multiple of the computed, system-specific average sync time. For
     * example, if the average is computed to be 32.5 ms and the multipler is
     * 1.8, then tasks are created with a period of
     * @param duration The number of milliseconds that the foreground Thread
     * should sleep while the specified number of WriteAndSync tasks are running
     * in the background thread
     */
    public void test(final int taskCount,
            final double taskPeriodMultiplier,
            final long duration) throws Exception {

        println();
        println("****************************************");
        println("*    org.hsqldb.lib.HsqlTimer tests    *");
        println("****************************************");
        println();

        int attemptedSyncs = getIntProperty(PK_AVG_SYNC_TIME_ATTEMPTED_SYNCS, 1024);
        int maxThreads = getIntProperty(PK_AVG_SYNC_TIME_MAX_THREADS, 2);
        int syncBufferSize = getIntProperty(PK_AVG_SYNC_TIME_BUFFSIZE,4096 * 1024);

        println("Computing system-specific avgerage file sync time.");
        println();
        println("Provided parameters:");
        println("--------------------------------------.");
        println("# File Syncs To Attempt               : " + attemptedSyncs + ".");
        println("# Bytes Written Per File Sync Attempt : " + syncBufferSize + ".");
        println("--------------------------------------.");

        // TODO:  for better accuracy, use a number of files located across
        //        the entire disk surface.

        double avgSyncTimeMillis = avgSyncTime(attemptedSyncs, maxThreads, syncBufferSize);
        double avgSyncsPerSecond = 1000D / avgSyncTimeMillis;
        double avgBytesSyncedPerSecond = syncBufferSize * avgSyncsPerSecond;
        double minAvgPeriod = (taskCount * avgSyncTimeMillis);
        long period = Math.max(1, Math.round(avgSyncTimeMillis * taskPeriodMultiplier));

        println("-------------------------------------------------.");
        println("System Average Sync Duration (parallel)          : " + avgSyncTimeMillis + " ms.");
        println("System Average Syncs / Second (parallel)         : " + Math.round(avgSyncsPerSecond) + ".");
        println("System Average Bytes Sync'ed / Second (parallel) : " + Math.round(avgBytesSyncedPerSecond) + ".");
        println("-------------------------------------------------.");
        println("Requested Concurrent Timer Tasks                 : " + taskCount);
        println("Requested Task Period Mutiplier                  : " + taskPeriodMultiplier + " (Relative To System Specific Sync Duration)");
        println("Requested Total Test Duration                    : " + duration + " ms.");
        println("-------------------------------------------------.");
        println("Effective Requested Task Period                  : " + period + " ms. (System Average Sync Duration * Requested Task Period Mutiplier)");
        println("Min. Time To Execute Each Task Once (serial)     : " + minAvgPeriod + " ms. (Requested Concurrent Timer Tasks * Avg File Sync Time, assuming 0 tasks starved, etc)");
        println("-------------------------------------------------.");


        if (period <= minAvgPeriod || minAvgPeriod >= duration) {
            double idealAvgRuns = (duration / minAvgPeriod);

            println("Idealized Avg. Runs / Task       : " + ((float) idealAvgRuns) + " (assuming no additional thread/process/file system overhead)");
        } else {
            double remainingDuration = (duration - minAvgPeriod);
            double remainingRuns = (remainingDuration / period);
            double idealAvgRuns = (1D + remainingRuns);

            println("Theoretical first cycle time  (min) : " + minAvgPeriod);
            println("Remaining duration                  : " + remainingDuration);
            println("Remaining runs                      : " + remainingRuns);
            println("Idealized avg. runs per task        : " + idealAvgRuns);
            println("(1 + (requested duration");
            println("      - theor. first cycle time");
            println("      ) / requested period)");
        }

        println("---------------------------------.");
        println();
        println("Running Timer Implementation and ");
        println("Computing Performance Stats. ");
        println();
        println("Please Wait...");
        println();


        System.runFinalization();
        System.gc();
        testJavaUtilTimer(taskCount, period, duration);

        System.runFinalization();
        System.gc();
        testHsqlTimer(taskCount, period, duration);
    }

    /**
     * Runs the java.util.Timer test using the given arguments. <p>
     *
     * @param taskCount the number of WriteAndSync tasks to add
     * @param periodMultiplier the period with with to schedule the tasks, as a
     * multiple of the computed, system-specific average sync time.
     * @param duration The number of milliseconds that the foreground Thread
     * should sleep while the specified number of WriteAndSync tasks are running
     * in the background thread
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public void testJavaUtilTimer(final int taskCount,
            final long period,
            final long duration) {

        println();
        println("****************************************");
        println("*            java.util.Timer           *");
        println("****************************************");
        println();

        WriteAndSyncTask.serial = 0;

        final java.util.Timer timer = new java.util.Timer();
        final WriteAndSyncTask[] tasks = new WriteAndSyncTask[taskCount];

        for (int i = 0; i < taskCount; i++) {
            tasks[i] = new WriteAndSyncTask();
            timer.scheduleAtFixedRate(tasks[i], 0, period);
        }

        final long start = HsqlTimer.now();

        try {
            Thread.sleep(duration);
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < tasks.length; i++) {
            tasks[i].cancel();
        }

        timer.cancel();

        final long elapsed = HsqlTimer.now() - start;

        println("Actual test duration: " + elapsed + " ms.");
        println();

        printTaskStats(tasks);
    }

    /**
     * Runs the HsqlTimer test using the given arguments. <p>
     *
     * @param taskCount the number of WriteAndSync tasks to add
     * @param periodMultiplier the period with with to schedule the tasks, as a
     * multiple of the computed, system-specific average sync time.
     * @param duration The number of milliseconds that the foreground Thread
     * should sleep while the specified number of WriteAndSync tasks are running
     * in the background thread
     */
    @SuppressWarnings({"CallToThreadDumpStack", "static-access"})
    public void testHsqlTimer(final int taskCount,
            final long period,
            final long duration) {

        println();
        println("****************************************");
        println("*       org.hsqldb.lib.HsqlTimer       *");
        println("****************************************");
        println();

        WriteAndSyncTask.serial = 0;

        final HsqlTimer timer = new HsqlTimer();
        final WriteAndSyncTask[] tasks = new WriteAndSyncTask[taskCount];
        final Object[] ttasks = new Object[taskCount];

        for (int i = 0; i < taskCount; i++) {
            tasks[i] = new WriteAndSyncTask();
            ttasks[i] = timer.schedulePeriodicallyAfter(0, period, tasks[i], true);
        }

        final long start = HsqlTimer.now();

        try {
            Thread.sleep(duration);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Thread timerThread = timer.getThread();

        for (int i = 0; i < taskCount; i++) {
            timer.cancel(ttasks[i]);
        }

        try {
            timerThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        final long elapsed = HsqlTimer.now() - start;

        println("Actual test duration: " + elapsed + " ms.");
        println();

        printTaskStats(tasks);

    }

    void printTaskStats(WriteAndSyncTask[] tasks) {
        float avgTotal = 0;
        int avgCount = 0;
        int starved = 0;
        int runs = 0;
        Stats periodStats = new Stats();
        Stats runStats = new Stats();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i].runs > 1) {
                double avgPeriod = tasks[i].getAveragePeriod();
                periodStats.addDataPoint(avgPeriod);
                avgTotal += avgPeriod;
                avgCount++;
            }
            runs += tasks[i].runs;
            if (tasks[i].runs == 0) {
                starved++;
            }
            runStats.addDataPoint(tasks[i].runs);
            tasks[i].release();
        }

        float periodAvg = (avgTotal / avgCount);
        float periodMax = (float) periodStats.getMax();
        int periodMaxCnt = 0;
        float periodMin = (float) periodStats.getMin();
        int periodMinCnt = 0;
        float periodRange = (periodMax - periodMin);
        float periodStddev = (float) periodStats.getStdDev();
        float periodGMean = (float) periodStats.getGeometricMean();
        float periodStddevR = (periodRange / periodStddev);

        float runsAvg = (runs / (float) tasks.length);
        int runsMin = Math.round((float) runStats.getMin());
        int runsMinCnt = 0;
        int runsMax = Math.round((float) runStats.getMax());
        int runsMaxCnt = 0;
        int runsRange = (runsMax - runsMin);
        float runsStddev = (float) runStats.getStdDev();
        float runsGMean = (float) runStats.getGeometricMean();
        float runsStddevR = (runsRange / runsStddev);

        for (int i = 0; i < tasks.length; i++) {
            double avgPeriod = tasks[i].getAveragePeriod();

            if (avgPeriod == periodMin) {
                periodMinCnt++;
            }

            if (avgPeriod == periodMax) {
                periodMaxCnt++;
            }

            if (tasks[i].runs == runsMin) {
                runsMinCnt++;
            }

            if (tasks[i].runs == runsMax) {
                runsMaxCnt++;
            }
        }

        println("------------------------");
        println("Starved tasks (runs = 0): " + starved + " (" + ((100 * starved) / tasks.length) + "%)");
        println("------------------------");
        println("Period                  :");
        println("------------------------");
        println("Average                 : " + periodAvg);
        println("~Minimum (count/runs)   : " + periodMin + " (" + periodMinCnt + "/" + tasks.length + ")");
        println("~Maximum (count/runs)   : " + periodMax + " (" + periodMaxCnt + "/" + tasks.length + ")");
        println("~Range                  : " + periodRange);
        println("Geometric mean          : " + periodGMean);
        println("Stddev                  : " + periodStddev);
        println("~Range/Stddev           : " + periodStddevR);
        println("------------------------");
        println("Runs                    :");
        println("------------------------");
        println("Average                 : " + runsAvg);
        println("Minimum (count/runs)    : " + runsMin + " (" + runsMinCnt + "/" + tasks.length + ")");
        println("Maximum (count/runs)    : " + runsMax + " (" + runsMaxCnt + "/" + tasks.length + ")");
        println("Range                   : " + runsRange);
        println("Geometric mean          : " + runsGMean);
        println("Stddev                  : " + runsStddev);
        println("Range/Stddev            : " + runsStddevR);
        println("------------------------");
    }

    public static Test suite() {
        return new TestSuite(HsqlTimerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
