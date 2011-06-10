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

import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

@ForSubject(org.hsqldb.lib.HsqlTimer.class)
public class HsqlTimerTest extends BaseTestCase {
    /**
     * Computes the system-specific average {@link java.io.FileDescriptor#sync()
     * sync} time.
     *
     * @param runs iterations to perform when computing the average
     * @param buff the data to write before each sync call
     * @return the total time to write buff and call sync runs times,
     *    divided by runs
     */
    static long syncTime(int runs, byte[] buff) {
        java.io.File             file = null;
        java.io.FileOutputStream fos;
        java.io.FileDescriptor   fd;
        long                     start = System.currentTimeMillis();

        try {
            file = java.io.File.createTempFile("SyncTest", ".tmp");
            fos  = new java.io.FileOutputStream(file);
            fd   = fos.getFD();

            for (int i = 0; i < runs; i++) {
                fos.write(buff);
                fos.flush();
                fd.sync();
            }

            long elapsed = System.currentTimeMillis() - start;

            return elapsed;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (file != null) {
                file.delete();
            }
        }
    }

    /**
     * WRITE_DELAY simulation task.
     *
     * Writes a given buffer to disk, sync's the associated file
     * descriptor and maintains an account of the average period
     * between executions.
     */
    static class WriteAndSyncTask extends java.util.TimerTask {
        // static
        /** Used to make the name of each task unique. */
        static int          serial;
        /** The data to write. */
        static final byte[] buf = new byte[256];

        // instance
        /** Identifies this task. */
        String                   name;
        /** The time at which this task was last executed. */
        long                     last;
        /** A running sum of the periods between executions. */
        long                     total;
        /** The number of times this task has been executed. */
        int                      runs;
        /** True until this task is the first time. */
        boolean                  firstTime = true;
        /** The file to write. */
        java.io.File             file;
        /** The FileOutputStream to write. */
        java.io.FileOutputStream fos;
        /** The FileDescriptor to sync. */
        java.io.FileDescriptor   fd;

        /** Constructs a new WriteAndSyncTask */
        WriteAndSyncTask() {
            this.name = "Task." + serial++;

            try {
                this.file = java.io.File.createTempFile(name, ".tmp");
                this.fos  = new java.io.FileOutputStream(file);
                this.fd   = fos.getFD();
            } catch(java.io.IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        /**
         * Runnable implementation. <p>
         *
         * Does the average period accounting and
         * invokes the writeAndSync method.
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
         * Writes a given buffer to disk and syncs the associated file
         * descriptor.
         */
        @SuppressWarnings("CallToThreadDumpStack")
        void writeAndSync() {
            try {
                this.fos.write(buf);
                this.fos.flush();
                this.fd.sync();
                Thread.sleep(1);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Closes the FileOutputStream, deletes the file
         * and nullifies Object fields.
         */
        @SuppressWarnings("CallToThreadDumpStack")
        public void release() {
            try {
                this.fos.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
            try {
                this.file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }

            this.fos  = null;
            this.file = null;
            this.fd   = null;
        }

        /**
         * Retrieves the computed moment of actual average periodicity
         * experienced by this task.
         */
        public float getAveragePeriod() {
            return (this.runs < 2) ? Float.NaN
                                   : (this.total/(float)(this.runs - 1));
        }


        /**
         * @return the String representation of this task, indicating
         *      its name, the number of runs so far and the
         *      computed moment of actual average periodicity
         *      experienced so far.
         */
        @Override
        public String toString()  {
            return this.name
                    + "["
                    + "runs: " + runs + ", "
                    + "actual avg. period: " + getAveragePeriod()
                    +  "]";
        }
    }

    static class Stats {
        double  min;
        double  max;
        double  pk;
        double  sk;
        double  vk;
        long    n;
        boolean initialized;
        boolean sample;

        void addDataPoint(double x) {

            double xi;
            double xsi;
            long   nm1;

            xi = x;

            if (!initialized) {
                n           = 1;
                pk          = xi;
                sk          = xi;
                min         = xi;
                max         = xi;
                vk          = 0.0;
                initialized = true;

                return;
            }

            n++;

            nm1 = (n - 1);
            xsi = (sk - (xi * nm1));
            vk  += ((xsi * xsi) / n) / nm1;
            sk  += xi;

            if (xi != 0) {
                pk  *= xi;
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
            return initialized ? Math.pow(pk, 1/(double)n) : Double.NaN;
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
     * @param args Currently unused
     */
    @OfMethod("<<ALL_METHODS>>")
    public void testHsqlTimer() {
        if (!getBooleanProperty("test.org.hsqldb.lib.HsqlTimer", true)){
            println("DISABLED: org.hsqldb.lib.HsqlTimerTest.");
            return;
        }
        // number of tasks to queue
        int    taskCount         = 100;
        // period, as a multiple of computed system-specific avg. sync time
        double periodMultiplier  = 1.4D;
        // how long to run the timer, in milliseconds
        long   duration          = 5000; // millis

        test(taskCount, periodMultiplier, duration);
    }

    /**
     * Runs the HsqlTimer and java.util.Timer tests using the given
     * arguments. <p>
     *
     * @param taskCount the number of WriteAndSync tasks to add
     * @param periodMultiplier the period with with to schedule
     *      the tasks, as a multiple of the computed, system-specific
     *      average sync time.
     * @param duration The number of milliseconds that the foreground
     *      Thread should sleep while the specified number of WriteAndSync
     *      tasks are running in the background thread
     */
    public static void test(final int taskCount,
                            final double periodMultiplier,
                            final long duration) {

        System.out.println();
        System.out.println("****************************************");
        System.out.println("*    org.hsqldb.lib.HsqlTimer tests    *");
        System.out.println("****************************************");
        System.out.println();

        int syncReps = 1024;
        int syncCapactity = 128*1024;
        System.out.println("Computing system-specific avg. fsync time...");
        System.out.println();
        System.out.println("---------------------------------.");
        System.out.println("FSync Repetitions                : " + syncReps + ".");
        System.out.println("Bytes Written Per FSync          : " + syncCapactity + ".");
        System.out.println("---------------------------------.");
        System.out.println();
        System.out.println("Please wait...");
        System.out.println();

        // TODO:  use a number of files located across the entire disk surface.
        long syncTime = syncTime(syncReps, new byte[syncCapactity]);
        double avgSyncTime = (syncTime /(double)syncReps);
        double syncTimeSeconds = syncTime / 1000D;
        double syncsPerSecond =   syncReps / syncTimeSeconds;
        double bytesPerSecond = syncCapactity * syncsPerSecond;
        double minAvgPeriod = (taskCount * avgSyncTime);
        long   period        = Math.round(avgSyncTime * periodMultiplier);
        
        System.out.println("Done.");
        System.out.println();
        System.out.println("---------------------------------.");
        System.out.println("Total FSync Time                 : " + syncTime + " ms.");
        System.out.println("Avg. FSyncs / Second             : " + Math.round(syncsPerSecond) + ".");
        System.out.println("Avg. Bytes FSync'ed / Second     : " + Math.round(bytesPerSecond) + ".");
        System.out.println("System-specific FSync Period     : " + avgSyncTime + " ms.");
        System.out.println("---------------------------------.");
        System.out.println("Requested Concurrent Timer Tasks : " + taskCount);
        System.out.println("Requested Task Period Mutiplier  : " + periodMultiplier);
        System.out.println("Requested Test Duration          : " + duration + " ms.");
        System.out.println("---------------------------------.");
        System.out.println("Effective Requested Task Period  : " + period + " ms. (fync period * multiplier)" );
        System.out.println("Min. Possible Avg. Task Period   : " + minAvgPeriod + " ms. (0 tasks starved)" );
        System.out.println("---------------------------------.");
       

        if (period <= minAvgPeriod || minAvgPeriod >= duration) {
            double idealAvgRuns = (duration / minAvgPeriod);

            System.out.println("Idealized Avg. Runs / Task       : " + (float)idealAvgRuns + "(no thread/process overhead)");
        } else {
            double remainingDuration = (duration - minAvgPeriod);
            double remainingRuns     = (remainingDuration / period);
            double idealAvgRuns      = (1D + remainingRuns);

            System.out.println("Theoretical first cycle time      : " + minAvgPeriod);
            System.out.println("Remaining duration                : " + remainingDuration);
            System.out.println("Remaining runs                    : " + remainingRuns);
            System.out.println("Idealized avg. runs per task      : " + idealAvgRuns);
            System.out.println("(1 + (requested duration");
            System.out.println("      - theor. first cycle time");
            System.out.println("      ) / requested period)");
        }

        System.out.println("---------------------------------.");
        System.out.println();
        System.out.println("Running Timer Implementation and ");
        System.out.println("Computing Performance Stats. ");
        System.out.println();
        System.out.println("Please Wait...");
        System.out.println();

        System.runFinalization();
        System.gc();
        testHsqlTimer(taskCount, period, duration);
        System.runFinalization();
        System.gc();
        testJavaUtilTimer(taskCount, period, duration);
    }


    /**
     * Runs the java.util.Timer test using the given arguments. <p>
     *
     * @param taskCount the number of WriteAndSync tasks to add
     * @param periodMultiplier the period with with to schedule
     *      the tasks, as a multiple of the computed, system-specific
     *      average sync time.
     * @param duration The number of milliseconds that the foreground
     *      Thread should sleep while the specified number of WriteAndSync
     *      tasks are running in the background thread
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public static void testJavaUtilTimer(final int taskCount,
                                         final long period,
                                         final long duration) {

        System.out.println();
        System.out.println("****************************************");
        System.out.println("*            java.util.Timer           *");
        System.out.println("****************************************");
        System.out.println();

        WriteAndSyncTask.serial = 0;

        final java.util.Timer    timer  = new java.util.Timer();
        final WriteAndSyncTask[] tasks  = new WriteAndSyncTask[taskCount];

        for (int i = 0; i < taskCount; i++) {
            tasks[i]  = new WriteAndSyncTask();
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

        System.out.println("Actual test duration: " + elapsed + " ms.");
        System.out.println();

        printTaskStats(tasks);
    }

    /**
     * Runs the HsqlTimer test using the given arguments. <p>
     *
     * @param taskCount the number of WriteAndSync tasks to add
     * @param periodMultiplier the period with with to schedule
     *      the tasks, as a multiple of the computed, system-specific
     *      average sync time.
     * @param duration The number of milliseconds that the foreground
     *      Thread should sleep while the specified number of WriteAndSync
     *      tasks are running in the background thread
     */
    @SuppressWarnings({"CallToThreadDumpStack", "static-access"})
    public static void testHsqlTimer(final int taskCount,
                                     final long period,
                                     final long duration) {

        System.out.println();
        System.out.println("****************************************");
        System.out.println("*       org.hsqldb.lib.HsqlTimer       *");
        System.out.println("****************************************");
        System.out.println();

        WriteAndSyncTask.serial = 0;

        final HsqlTimer          timer  = new HsqlTimer();
        final WriteAndSyncTask[] tasks  = new WriteAndSyncTask[taskCount];
        final Object[]           ttasks = new Object[taskCount];

        for (int i = 0; i < taskCount; i++) {
            tasks[i]  = new WriteAndSyncTask();
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

        System.out.println("Actual test duration: " + elapsed + " ms.");
        System.out.println();

        printTaskStats(tasks);

    }

    static void printTaskStats(WriteAndSyncTask[] tasks) {
        float avgTotal    = 0;
        int   avgCount    = 0;
        int   starved     = 0;
        int   runs        = 0;
        Stats periodStats = new Stats();
        Stats runStats    = new Stats();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i].runs > 1) {
                double avgPeriod = tasks[i].getAveragePeriod();
                periodStats.addDataPoint(avgPeriod);
                avgTotal += avgPeriod;
                avgCount++;
            }
            runs  += tasks[i].runs;
            if (tasks[i].runs == 0) {
                starved++;
            }
            runStats.addDataPoint(tasks[i].runs);
            tasks[i].release();
        }

        float periodAvg      = (avgTotal / avgCount);
        float periodMax      = (float) periodStats.getMax();
        int   periodMaxCnt   = 0;
        float periodMin      = (float) periodStats.getMin();
        int   periodMinCnt   = 0;
        float periodRange    = (periodMax - periodMin);
        float periodStddev   = (float)periodStats.getStdDev();
        float periodGMean    = (float)periodStats.getGeometricMean();
        float periodStddevR  = (periodRange / periodStddev);

        float runsAvg      = (runs / (float)tasks.length);
        int   runsMin      = Math.round((float)runStats.getMin());
        int   runsMinCnt   = 0;
        int   runsMax      = Math.round((float)runStats.getMax());
        int   runsMaxCnt   = 0;
        int   runsRange    = (runsMax - runsMin);
        float runsStddev   = (float) runStats.getStdDev();
        float runsGMean    = (float) runStats.getGeometricMean();
        float runsStddevR  = (runsRange / runsStddev);

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

        System.out.println("------------------------");
        System.out.println("Starved tasks (runs = 0): " + starved + " (" + ((100*starved)/tasks.length) + "%)");
        System.out.println("------------------------");
        System.out.println("Period                  :");
        System.out.println("------------------------");
        System.out.println("Average                 : " + periodAvg);
        System.out.println("~Minimum (count/runs)   : " + periodMin + " (" + periodMinCnt + "/" + tasks.length + ")");
        System.out.println("~Maximum (count/runs)   : " + periodMax + " (" + periodMaxCnt + "/" + tasks.length + ")");
        System.out.println("~Range                  : " + periodRange);
        System.out.println("Geometric mean          : " + periodGMean);
        System.out.println("Stddev                  : " + periodStddev);
        System.out.println("~Range/Stddev           : " + periodStddevR);
        System.out.println("------------------------");
        System.out.println("Runs                    :");
        System.out.println("------------------------");
        System.out.println("Average                 : " + runsAvg);
        System.out.println("Minimum (count/runs)    : " + runsMin + " (" + runsMinCnt + "/" + tasks.length + ")");
        System.out.println("Maximum (count/runs)    : " + runsMax + " (" + runsMaxCnt + "/" + tasks.length + ")");
        System.out.println("Range                   : " + runsRange);
        System.out.println("Geometric mean          : " + runsGMean);
        System.out.println("Stddev                  : " + runsStddev);
        System.out.println("Range/Stddev            : " + runsStddevR);
        System.out.println("------------------------");
    }


    public static Test suite() {
        TestSuite suite = new TestSuite(HsqlTimerTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
