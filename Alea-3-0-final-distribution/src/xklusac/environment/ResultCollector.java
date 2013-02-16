/*
 * ResultCollector.java
 *
 * Created on 4. listopad 2009, 12:14
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package xklusac.environment;

import gridsim.*;
import gridsim.Gridlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import xklusac.extensions.Input;
import xklusac.extensions.Output;

/**
 * Class ResultCollector<p>
 * This class stores results into csv file(s) and graphs (future work).
 *
 * @author Dalibor Klusacek
 */
public class ResultCollector {

    public LinkedList results;
    public String data_set;
    Output out = new Output();
    String problem = "";
    double TSA = 0.0;
    double SAJ = 0.0;
    double SDJ = 0.0;
    double SSDJ = 0.0;
    int succ_m = 0;
    int bad = 0;
    double job_time = 0.0;
    double avail_time = 0.0;
    //static double failure_time = 0.0;
    double wjob_time = 0.0;
    double wavail_time = 0.0;
    //static double wfailure_time = 0.0;
    double av_PEs = 0.0;
    double wav_PEs = 0.0;
    double day_usage = 0.0;
    double week_usage = 0.0;
    int week_count = 0;
    //double run_time = 0.0;
    /** denotes total flow time */
    double flow_time = 0.0;
    /** denotes total wait time */
    double wait_time = 0.0;
    /** auxiliary variable */
    private double sa_total = 0.0;
    /** auxiliary variable */
    private double slowdown = 0.0;
    /** auxiliary variable */
    private double awrt = 0.0;
    /** auxiliary variable */
    private double awsd = 0.0;
    /** auxiliary variable */
    private int failed;
    /** auxiliary variable */
    private int success;
    double succ_flow = 0.0;
    double succ_wait = 0.0;
    double succ_slow = 0.0;
    /** Total tardiness of this schedule */
    private double tardiness = 0.0;
    /** deadline score */
    private int real_score = 0;
    /** deadline score */
    private int neg_score = 0;
    /** auxiliary variable */
    private int received = 0;
    static Double[] slowdowns = new Double[5];
    static Double[] resp_times = new Double[6];
    static Double[] wait_times = new Double[6];
    private String user_dir = "";

    /** Creates a new instance of ResultCollector */
    public ResultCollector(LinkedList results, String prob) {
        if (ExperimentSetup.meta) {
            user_dir = "/scratch/xklusac/" + ExperimentSetup.path;
        } else {
            user_dir = System.getProperty("user.dir");
        }

        this.results = results;
        this.problem = prob;
        try {
            //System.out.println("!&&&&&&&&&&&&&&&&&& "+user_dir + "/Results("+problem+").csv");
            out.deleteResults(user_dir + "/Results(" + problem + ").csv");
            out.deleteResults(user_dir + "/SecRes(" + problem + ").csv");
            out.deleteResults(user_dir + "/WGraphs(" + problem + ").csv");
            out.deleteResults(user_dir + "/RGraphs(" + problem + ").csv");
            out.deleteResults(user_dir + "/SGraphs(" + problem + ").csv");
            out.deleteResults(user_dir + "/Fairness(" + problem + ").csv");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        clear(slowdowns);
        clear(resp_times);
        clear(wait_times);
    }

    /** generate results */
    public void generateHeader(String data_set) {
        String waxis = "algorithm";
        int whours = 0;
        for (int i = 0; i < 1442; i++) {
            if (i % 60 == 0) {
                waxis += "\t" + whours;
                whours++;
            } else {
                waxis += "\t ";
            }
        }

        String raxis = "algorithm";
        int rhours = 0;
        for (int i = 0; i < 2882; i++) {
            if (i % 60 == 0) {
                raxis += "\t" + rhours;
                rhours++;
            } else {
                raxis += "\t-";
            }
        }

        String saxis = "algorithm";
        for (int i = 0; i < 1001; i++) {
            saxis += "\t" + i;
        }
        saxis += "\t>1000";

        try {
            out.writeString(user_dir + "/Results(" + problem + ").csv", "1/" + data_set +
                    "\tsubmit.\tcompl.\tkilled\tresp_time\truntime\tsch-cr-time\tmakespan\tweigh_usg\tclass_usg\ttard\twait\tsd\tawrt\tawsd\ts_resp\ts_wait\ts_sld");
            out.writeString(user_dir + "/SecRes(" + problem + ").csv", "1/" + data_set +
                    "\tsld_1.1\tsld_10\tsld_100\tsld_1000\tsld_max\twait_1s\twait_1m\twait_1h\twait_12h\twait_24h\twait_max\tresp_1s\tresp_1m\tresp_1h\tresp_12h\tresp_24h\tresp_max");
            out.writeString(user_dir + "/WGraphs(" + problem + ").csv", waxis);
            out.writeString(user_dir + "/SGraphs(" + problem + ").csv", saxis);
            out.writeString(user_dir + "/RGraphs(" + problem + ").csv", raxis);
            out.writeString(user_dir + "/Fairness(" + problem + ").csv", data_set + "\tsld_min\tsld_max\tsld_avg\twait_min\twait_max\twait_avg");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /** generate results - stores into csv file */
    public void generateResults(String suff, int experiment_count) {
        // calculate and print output results for this setup of experiment
        double avg_deadline_score = 0.0;
        double avg_time = 0.0;
        double avg_makespan = 0.0;
        double wait_time = 0.0;
        double classic_usage = 0.0;
        int neg_score = 0;
        double flow_time = 0.0;
        double machine_usage = 0.0;
        double creation_time = 0.0;
        double tardiness = 0.0;
        double slowdown = 0.0;
        double awrt = 0.0;
        double awsd = 0.0;
        int submitted = 0;
        double succ_flow = 0.0;
        double succ_wait = 0.0;
        double succ_slow = 0.0;

        for (int i = 0; i < results.size(); i++) {

            // deadline score
            avg_deadline_score += (Integer) results.get(i);
            // scheduling time
            avg_time += (Double) results.get(i + 1);
            // makespan
            avg_makespan += (Double) results.get(i + 2);
            // classic machine usage
            classic_usage += (Double) results.get(i + 3);
            // scalability results
            wait_time += (Double) results.get(i + 4);
            // negative score
            neg_score += (Integer) results.get(i + 5);
            // flow time
            flow_time += (Double) results.get(i + 6);
            // weighted machine usage
            machine_usage += (Double) results.get(i + 7);
            // time to create schedule
            creation_time += (Double) results.get(i + 8);
            // tardiness
            tardiness += (Double) results.get(i + 9);
            // slowdown
            slowdown += (Double) results.get(i + 10);
            // avg. weigh. response time
            awrt += (Double) results.get(i + 11);
            // avg. weigh. slowdown
            awsd += (Double) results.get(i + 12);

            submitted += (Integer) results.get(i + 13);
            succ_flow += (Double) results.get(i + 14);
            succ_wait += (Double) results.get(i + 15);
            succ_slow += (Double) results.get(i + 16);
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
            i++;
        }
        // print results (deadline score and scheduling time and makespan)
        //System.out.println("DataSet: " + data_set);
        System.out.println("-----------------------------------------------------------------------------------------------------------");
        System.out.println(" ResultCollector - generates results for " + (Math.round(submitted * 100.0) / (experiment_count * 100.0)) + " submitted jobs.");
        //String fair = "user\ttuwt[i]\tnuwt[i]\ttusa[i]\tnwt";
        String fair = "";
        String jobs = "";

        if (ExperimentSetup.algID == ExperimentSetup.prevAlgID) {
            fair += "-------------------------------\n";
            jobs += "-------------------------------";
        }
        fair = fair + "" + predictFairness();

        //System.out.println(" Fairness = "+fair);
        System.out.println("-----------------------------------------------------------------------------------------------------------");
        String prob = "_";
        prob += ExperimentSetup.algID + "_" + ExperimentSetup.name;
        if (ExperimentSetup.estimates && ExperimentSetup.useDurationPrecision) {
            prob += "_" + ExperimentSetup.userPercentage;
        }
        if (ExperimentSetup.estimates && !ExperimentSetup.useDurationPrecision) {
            prob += "_estim";
        }
        if (!ExperimentSetup.estimates) {
            prob += "_exact";
        }

        try {
            // delete old one, will be left at the end
            if (ExperimentSetup.algID != ExperimentSetup.prevAlgID) {
                out.deleteResults(user_dir + "/Users" + prob + ".csv");
                out.deleteResults(user_dir + "/jobs" + prob + ".csv");
            }

            String end = addJobs((user_dir + "/jobs" + prob + ".csv"), jobs);
            jobs = end;
            System.out.println("jobs here = " + jobs);

            out.writeString(user_dir + "/Users" + prob + ".csv", fair);
            out.writeString(user_dir + "/jobs" + prob + ".csv", jobs);
            out.writeString(user_dir + "/Results(" + problem + ").csv", suff + "\t" +
                    Math.round(submitted * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(avg_deadline_score * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(neg_score * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(flow_time * 100) / (experiment_count * 100.0) + "\t" +
                    Math.round(avg_time * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(creation_time * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(avg_makespan) / (experiment_count) + "\t" +
                    Math.round(machine_usage * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(classic_usage * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(tardiness * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_time * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(slowdown * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(awrt * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(awsd * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(succ_flow * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(succ_wait * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(succ_slow * 100.0) / (experiment_count * 100.0));


            out.writeString(user_dir + "/SecRes(" + problem + ").csv", suff + "\t" +
                    Math.round(slowdowns[0] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(slowdowns[1] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(slowdowns[2] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(slowdowns[3] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(slowdowns[4] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[0] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[1] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[2] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[3] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[4] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(wait_times[5] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[0] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[1] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[2] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[3] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[4] * 100.0) / (experiment_count * 100.0) + "\t" +
                    Math.round(resp_times[5] * 100.0) / (experiment_count * 100.0));

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        generateJobsStatistics(suff, avg_deadline_score);

        results.clear();
        clear(slowdowns);
        clear(resp_times);
        clear(wait_times);
    }

    /**
     * Deletes results.
     */
    public void deleteSchedResults(String suff) {
        try {
            // delete files with old simulation results
            out.deleteResults(user_dir + "/actual_usage_" + suff + ".csv");
            out.deleteResults(user_dir + "/waiting_" + suff + ".csv");
            out.deleteResults(user_dir + "/running_" + suff + ".csv");
            out.deleteResults(user_dir + "/day_usage_" + suff + ".csv");
            out.deleteResults(user_dir + "/week_usage_" + suff + ".csv");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Stores all information about currently finished job.
     */
    public void addFinishedJobToResults(ComplexGridlet gridlet_received, ArrayList resourceInfoList) {
        GridletInfo gi = new GridletInfo(gridlet_received);
        received++;
        double finish_time = 0.0;
        double cpu_time = 0.0;
        double mips = 0.0;
        double arrival = 0.0;
        if (gridlet_received.getGridletStatus() == Gridlet.FAILED_RESOURCE_UNAVAILABLE || gridlet_received.getGridletStatus() == Gridlet.FAILED) {
            failed++;
            finish_time = Math.max(gi.getGridlet().getArrival_time(), (gi.getGridlet().getExecStartTime() + gi.getGridlet().getActualCPUTime()));
            cpu_time = Math.max(1.0, gi.getGridlet().getActualCPUTime());
            mips = gridlet_received.getGridletFinishedSoFar();
            arrival = gi.getGridlet().getArrival_time();
            System.out.println(gi.getID() + " returned failed, time = " + GridSim.clock());

        } else if (gridlet_received.getGridletStatus() == Gridlet.CANCELED) {
            failed++;
            finish_time = GridSim.clock();
            cpu_time = 0.0;
            arrival = gi.getGridlet().getArrival_time();
            mips = 0.0;
            System.out.println(gi.getID() + " returned canceled, time = " + GridSim.clock());

        } else {
            success++;
            ExperimentSetup.totalPower -= gi.getGridlet().getPower();
            finish_time = gi.getGridlet().getFinishTime();
            cpu_time = gi.getGridlet().getActualCPUTime();
            arrival = gi.getGridlet().getArrival_time();
            mips = gridlet_received.getGridletLength();
            double succ_resp = Math.max(0.0, (finish_time - arrival));
            succ_flow += succ_resp;
            succ_wait += Math.max(0.0, (succ_resp - cpu_time));
            // bacha zmena 1.0 -> 10.0
            succ_slow += Math.max(1.0, (succ_resp / Math.max(1.0, cpu_time)));
            calculateOtherResults(Math.max(1.0, (succ_resp / Math.max(1.0, cpu_time))), Math.max(0.0, (succ_resp - cpu_time)), succ_resp);
        }
        //run_time += cpu_time;

        gi.setTardiness(Math.max(0, finish_time - gridlet_received.getDue_date()));

        // calculate various results
        double g_tard = gi.getTardiness();
        tardiness += g_tard;
        if (g_tard <= 0.0) {
            real_score++;
        } else {
            neg_score++;
        }


        double response = Math.max(0.0, (finish_time - arrival));
        double saj = gi.getNumPE() * mips;



        // utilized time by job
        job_time += gi.getNumPE() * cpu_time;
        wjob_time += gi.getNumPE() * gridlet_received.getGridletFinishedSoFar();
        flow_time += response;
        wait_time += Math.max(0.0, (response - cpu_time));
        // slowdown must be >= than 1.0, response may be 0 so normalize
        slowdown += Math.max(1.0, (response / Math.max(1.0, cpu_time))); // prevent division by zero

        sa_total += saj;
        awrt += saj * response;
        awsd += saj * Math.max(1.0, ((response / Math.max(1.0, cpu_time)))); // prevent division by zero

        // write out job's result
        try {
            // giID - wait - runtime - userID - numPE
            out.writeString(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv", gridlet_received.getGridletID() + "\t" + Math.max(0.0, (response - cpu_time)) +
                    "\t" + cpu_time + "\t" + gi.getUser() + "\t" + gi.getNumPE());

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        for (int j = 0; j < resourceInfoList.size(); j++) {
            ResourceInfo ri = (ResourceInfo) resourceInfoList.get(j);
            if (gridlet_received.getResourceID() == ri.resource.getResourceID()) {
                // we lower the load of resource, update info about overall tardiness and exit cycle
                ri.lowerResInExec(gi);
                ri.prev_tard += g_tard;
                if (g_tard <= 0.0) {
                    ri.prev_score++;
                }
                break;
            }
        }

    }

    /**
     * Compute all results and stores them into a LinkedList.
     */
    public void computeResults(double av_PEs, double wav_PEs, double failure_time, double wfailure_time,
            double clock, double runtime, double classic_load, double max_load, int submitted) {
        avail_time = GridSim.clock() * av_PEs;
        avail_time -= failure_time;
        double usage = Math.round((job_time / avail_time) * 10000.0);

        wavail_time = GridSim.clock() * wav_PEs;
        wavail_time -= wfailure_time;
        double wusage = Math.round((wjob_time / wavail_time) * 10000.0);

        // successfully completed jobs
        results.add(success);
        // required sched. time
        results.add(clock / received);
        // makespan
        results.add(GridSim.clock());
        // classic machine usage
        results.add(usage / 100.0);
        //wait time
        wait_time = Math.round((wait_time / received) * 100) / 100.0;
        results.add(wait_time);

        // negative deadline score
        // now it is failed
        results.add(failed);
        // total flow time
        results.add(flow_time / received);
        // weighted machine usage

        results.add(wusage / 100.0);
        // avg. runtime
        results.add(runtime / received);
        // avg. tardiness
        results.add(tardiness / received);
        //av. slowdown
        results.add(slowdown / received);
        //av. weighted response time
        results.add(awrt / sa_total);
        //av. weigted slowdown
        results.add(awsd / sa_total);
        results.add(submitted);

        // values reflecting only successfully completed jobs
        results.add(succ_flow / success);
        results.add(succ_wait / success);
        results.add(succ_slow / success);

        finish(slowdowns);
        finish(resp_times);
        finish(wait_times);
    }

    /**
     * Resets all internal variables before new experiment starts.
     */
    public void reset() {
        this.success = 0;
        this.failed = 0;
        this.received = 0;
        this.job_time = 0.0;
        this.wjob_time = 0.0;
        this.tardiness = 0.0;
        this.succ_flow = 0.0;
        this.succ_slow = 0.0;
        this.succ_wait = 0.0;
        this.flow_time = 0.0;
        this.wait_time = 0.0;
        this.slowdown = 0.0;
        this.awrt = 0.0;
        this.awsd = 0.0;
        this.sa_total = 0.0;

        clear(slowdowns);
        clear(resp_times);
        clear(wait_times);

        ExperimentSetup.users.clear();
    }

    private void clear(Double[] field) {
        for (int i = 0; i < field.length; i++) {
            field[i] = 0.0;
        }
    }

    private void finish(Double[] field) {
        for (int i = 0; i < field.length; i++) {
            field[i] = (field[i] / success) * 100.0;
        }
    }

    private void calculateOtherResults(double sl, double wt, double rsp) {
        if (sl > 1000) {
            slowdowns[4]++;
        } else if (sl > 100) {
            slowdowns[3]++;
        } else if (sl > 10) {
            slowdowns[2]++;
        } else if (sl > 1.1) {
            slowdowns[1]++;
        } else if (sl > 0.0) {
            slowdowns[0]++;
        }

        if (wt > 86400.0) {
            wait_times[5]++;
        } else if (wt > 43200.0) {
            wait_times[4]++;
        } else if (wt > 3600.0) {
            wait_times[3]++;
        } else if (wt > 60.0) {
            wait_times[2]++;
        } else if (wt > 1.0) {
            wait_times[1]++;
        } else if (wt >= 0.0) {
            wait_times[0]++;
        }

        if (rsp > 86400.0) {
            resp_times[5]++;
        } else if (rsp > 43200.0) {
            resp_times[4]++;
        } else if (rsp > 3600.0) {
            resp_times[3]++;
        } else if (rsp > 60.0) {
            resp_times[2]++;
        } else if (rsp > 1.0) {
            resp_times[1]++;
        } else if (rsp >= 0.0) {
            resp_times[0]++;
        }

    }
    // generate entry into Graphs_* file using the trace stored in Jobs.csv file
    private void generateJobsStatistics(String suff, double job_count) {
        String line = "";
        Input r = new Input();
        BufferedReader br = r.openFile(new File(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv"));
        Double[] wt = new Double[1442];
        Double[] rt = new Double[2882];
        Double[] sd = new Double[1002];
        clear(wt);
        clear(rt);
        clear(sd);

        while (true) {
            try {
                line = br.readLine();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            if (line == null) {
                break;
            } else {
                String values[] = line.split("\t");
                double wait = Double.parseDouble(values[1]);
                double sld = Math.max(1.0, (Math.max(0.0, (wait + Double.parseDouble(values[2]))) / Math.max(1.0, Double.parseDouble(values[2]))));
                double resp = Math.max(0.0, (wait + Double.parseDouble(values[2])));
                // wait time in minutes
                wait = Math.round(wait / 60.0);
                resp = Math.round(resp / 60.0);
                Long inl = Math.round(wait);
                int index = Integer.valueOf(inl.intValue());

                Long inr = Math.round(resp);
                int rindex = Integer.valueOf(inr.intValue());

                Long sindexl = Math.round(sld);
                int sindex = Integer.valueOf(sindexl.intValue());

                // increase counter regarding the jobs
                if (index > 1440) {
                    wt[1441]++;
                } else {
                    wt[index]++;
                }

                if (rindex > 2880) {
                    rt[2881]++;
                } else {
                    rt[index]++;
                }

                // increase slowdown counter
                if (sindex > 1000) {
                    sd[1001]++;
                } else {
                    sd[sindex]++;
                }

                // handle users' fairness
                User u = ExperimentSetup.users.get(values[3]);
                wait = Double.parseDouble(values[1]);
                //System.out.println(ExperimentSetup.users.size()+" user = "+values[3]+" curr="+u);
                u.updateJobs(1.0);
                u.updateSlowdown(sld);
                u.updateWait(wait);
                u.updateRuntime(Double.parseDouble(values[2]) * Integer.parseInt(values[4]));
            }
        }

        //make the analysis and write it out to Graphs_*.csv
        line = suff;
        String sline = suff;
        String fline = suff;
        String rline = suff;
        double cdf = 0.0;
        double scdf = 0.0;
        double rcdf = 0.0;

        // wait time CDF
        for (int i = 0; i < wt.length; i++) {
            double percent = wt[i] / job_count;
            cdf += percent;
            line += "\t" + cdf;
        }
        for (int i = 0; i < rt.length; i++) {
            double percent = rt[i] / job_count;
            rcdf += percent;
            rline += "\t" + rcdf;
        }
        // slowdown CDF
        for (int i = 0; i < sd.length; i++) {
            double spercent = sd[i] / job_count;
            scdf += spercent;
            sline += "\t" + scdf;
        }

        // calculate fairness
        double min_sld = Double.MAX_VALUE;
        double max_sld = -1.0;
        double avg_sld = 0.0;

        double min_wait = Double.MAX_VALUE;
        double max_wait = -1.0;
        double avg_wait = 0.0;

        Enumeration keys = ExperimentSetup.users.keys();
        for (int i = 0; i < ExperimentSetup.users.size(); i++) {
            User u = ExperimentSetup.users.get(keys.nextElement());
            // slowdown and wait time normalized by user utilized runtime
            double us = u.getSlowdown() / u.getRuntime();
            double uw = u.getWait() / u.getRuntime();

            if (min_sld > us) {
                min_sld = us;
            }
            if (max_sld < us) {
                max_sld = us;
            }
            if (min_wait > uw) {
                min_wait = uw;
            }
            if (max_wait < uw) {
                max_wait = uw;
            }
            avg_sld += us;
            avg_wait += uw;
        }

        avg_sld = avg_sld / ExperimentSetup.users.size();
        avg_wait = avg_wait / ExperimentSetup.users.size();
        fline += "\t" + min_sld + "\t" + max_sld + "\t" + avg_sld + "\t" + min_wait + "\t" + max_wait + "\t" + avg_wait;

        // write out job's result
        try {
            out.writeString(user_dir + "/WGraphs(" + problem + ").csv", line);
            out.writeString(user_dir + "/RGraphs(" + problem + ").csv", rline);
            out.writeString(user_dir + "/SGraphs(" + problem + ").csv", sline);
            out.writeString(user_dir + "/Fairness(" + problem + ").csv", fline);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // delete job trace after each experiment
        try {
            out.deleteResults(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    private String addJobs(String filename, String text) {
        String line = "";
        String output = "";
        Input r = new Input();
        BufferedReader br = r.openFile(new File(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv"));
        int c = 0;

        while (true) {
            try {
                line = br.readLine();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            if (line == null) {
                break;
            } else {
                c++;
            }
        }
        System.out.println("------------- There are " + c + " jobs now, file = " + filename);
        r.closeFile(br);
        br = r.openFile(new File(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv"));
        int k = 0;
        if (!text.equals("")) {
            try {
                out.writeString(filename, text);
            } catch (IOException ex) {
                Logger.getLogger(ResultCollector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        while (true) {
            try {
                line = br.readLine();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            if (line == null) {
                break;
            } else {
                k++;
                String[] values = line.split("\t");
                line = values[0] + "\t" + round(Double.parseDouble(values[1])) + "\t" + values[2] + "\t" + values[3];
                if (c == k) {
                    output += line + "";
                } else {
                    try {
                        out.writeString(filename, line);
                    } catch (IOException ex) {
                        Logger.getLogger(ResultCollector.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        r.closeFile(br);
        return output;
    }

    private String predictFairness() {
        double fairness = 0;
        String line = "";
        double[] nuwt = new double[Scheduler.users.size()];
        double[] tuwt = new double[Scheduler.users.size()];
        double[] tusa = new double[Scheduler.users.size()];
        double[] tujobs = new double[Scheduler.users.size()];
        double nwt = 0.0;

        // load known values
        for (int i = 0; i < Scheduler.users.size(); i++) {
            nuwt[i] = 0.0;
            tuwt[i] = Scheduler.total_uwt.get(i);
            tusa[i] = Scheduler.users_time.get(i);
            tujobs[i] = Scheduler.users_jobs.get(i) + Scheduler.users_P_jobs.get(i);
        }
        // now tuwt and tusa stores both known and predicted values
        // now proceed with fairness computation        
        for (int i = 0; i < Scheduler.users.size(); i++) {
            nuwt[i] = tuwt[i] / Math.max(1.0, tusa[i]);
            nwt += nuwt[i];
        }
        nwt = nwt / (1.0 * Scheduler.users.size());
        int totj = 0;

        // calculate the sum of powers of average normalized wt - normalized user wt
        for (int i = 0; i < Scheduler.users.size(); i++) {
            // to avoid decreasement of values when the power is computed we add 1.0 
            if (i < Scheduler.users.size() - 1) {
                line += Scheduler.users.get(i) + "\t" + tuwt[i] + "\t" + nuwt[i] + "\t" + tusa[i] + "\t" + tujobs[i] + "\n";
            } else {
                line += Scheduler.users.get(i) + "\t" + tuwt[i] + "\t" + nuwt[i] + "\t" + tusa[i] + "\t" + tujobs[i] + "";
            }
            totj += tujobs[i];
            //System.out.println("user"+i+" nuwt="+(Math.round(nuwt[i]*100.0))/100.0+" tusa="+Math.round(tusa[i]));            
            fairness += Math.pow((1.0 + (nwt - nuwt[i])), 2.0);
        }
        System.out.println("nwt = " + nwt + " tot  jobs = " + totj);
        //System.out.println("current fairness = "+Math.round(fairness));        
        //System.out.println("current fairness = "+fairness);        
        return line;
    }

    private double round(double d) {
        d = Math.round(d * 10000.0) * 1.0 / 10000.0;
        return d;
    }
}
