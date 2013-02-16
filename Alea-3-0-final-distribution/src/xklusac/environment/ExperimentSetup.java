package xklusac.environment;

import xklusac.algorithms.*;
import eduni.simjava.Sim_system;
import java.io.IOException;
import java.util.*;
import gridsim.*;
import java.awt.Color;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import xklusac.extensions.*;

/**
 * Class ExperimentSetup <p>
 * This is the main class. It creates all entities, such as Scheduler, Resources and and performs
 * the whole experiment. <br>It allows to run multiple passes with different settings such as job parameters, data-sets
 * and algorithms. This parameters has to be properly set manually. The results are printed on the screen and also stored in a text files
 * when the simulation is finished. Information about various objectives (machine usage, slowdown, deadlines,...) are stored.<p>
 * Alea 3.0 supports <b>GRID WORKLOADS FORMAT (GWF)</b> which is described at the <b>GRID WORKLOAD ARCHIVE (GWA)</b>: <a href="http://gwa.ewi.tudelft.nl">http://gwa.ewi.tudelft.nl</a><p>
 * Alea 3.0 supports <b>STANDARD WORKLOADS FORMAT (SWF)</b> which is described at the <b>PARALLEL WORKLOADS ARCHIVE (GWA)</b>: <a href="http://www.cs.huji.ac.il/labs/parallel/workload/">http://www.cs.huji.ac.il/labs/parallel/workload/</a><p>
 * Alea 3.0 supports <b>MetaCentrum WORKLOAD FORMAT (MWF)</b> which is described at: <a href="http://www.fi.muni.cz/~xklusac/workload/">http://www.fi.muni.cz/~xklusac/workload/</a><p>
 * Alea 3.0 supports <b>PISA WORKLOAD FORMAT (PWF)</b> which is described at: <a href="http://www.fi.muni.cz/~xklusac/alea/">http://www.fi.muni.cz/~xklusac/alea/</a><p>
 * 
 * Most recent versions of <b>Alea 3.0</b> are available at: <a href="http://www.fi.muni.cz/~xklusac/alea">http://www.fi.muni.cz/~xklusac/alea</a><br>
 * To run the simulation with Alea 3.0, Java 1.6 or newer is needed and the latest GridSim should be used.
 *
 * @author Dalibor Klusacek
 */
public class ExperimentSetup {

    public static double powerCap = 50.00;
    public static double totalPower = 0.00;
    /** bandwith */
    static double baudRate = 10000;
    /** total count of Job Submission System */
    static int entities = 1;
    /** max. PE count at the "biggest" resource */
    static int maxPE = 1;
    /** min. PE rating of the slowest resource */
    static int minPErating = Integer.MAX_VALUE;
    static int maxPErating = 1;
    /** names of clusters */
    static LinkedList clusterNames = new LinkedList();
    /** names of machines */
    static LinkedList machineNames = new LinkedList();
    /** set to true if visualization should be shown. <br>
     * Be carefull, visualization requires some overhead which may slow down the simulation. Use only for testing or if you want to obtain graphical output.
     */
    static boolean visualize = true;
    /** set true to use specific job requirements */
    static boolean reqs;
    /** set true to use job runtime estimates */
    static boolean estimates;
    /** set true to use failure trace - if available */
    static boolean failures;
    /** set true to use avg. job length as an runtime estimate */
    static boolean useAvgLength;
    /** set true to use last job runtime as a new runtime estimate */
    static boolean useLastLength;
    /** set true to use on-demand LS-based optimization */
    static boolean useEventOpt;
    /** auxiliary variable */
    static boolean useUserPrecision;
    /** auxiliary variable */
    static boolean useDurationPrecision;
    /** auxiliary variable */
    static boolean meta;
    /**  auxiliary variable */
    static boolean data = false;
    /** auxiliary variable */
    static boolean useHeap = false;
    /** auxiliary variable */
    static int userPercentage;
    /** andom number generator seed */
    static int rnd_seed;
    /** auxiliary variable */
    static int max_estim;
    /** auxiliary variable */
    static String path;
    /** auxiliary variable */
    static Hashtable<String, User> users = new Hashtable<String, User>();
    /** multiplies the number of iterations of opt. algorithms */
    public static int multiplicator;
    /** auxiliary variable */
    static int gap_length;
    /** auxiliary variable */
    static int algID = 0;
    /** auxiliary variable */
    static int prevAlgID = -1;
    /** auxiliary variable */
    static String name = "";
    /** the weight of fairness criterion in objective function */
    public static int fair_weight;
    public static SchedulingPolicy policy = null;
    public static OptimizationAlgorithm opt_alg = null;
    public static OptimizationAlgorithm fix_alg = null;
    public static boolean use_compresion = false;

    /**
     * The main method - create all entities and starts the simulation. <br>
     * It is also capable of multiple starts of the simulation with different setup (machine count, job parameters, data sets).
     */
    public static void main(String[] args) {
        // if required - start the graphical output using -v parameter
        if (args.length > 0) {
            if (args[0].equals("-v")) {
                visualize = true;
            }
        } else {
            // change this to true if you want to visualize always, disregarding parameters.
            visualize = false;
        }
        // stores references to animation windows
        LinkedList<Visualizator> windows = new LinkedList();
        // if true then create windows with graps.
        if (visualize) {
            createGUI(windows);
        }

        // list of results
        LinkedList results = new LinkedList();

        // data set name(s) are stored in this list. Typically, the data are expected to be in a "$PATH/data-set/" directory, where $PATH is the path to where the Alea directory is.
        // Therefore, this directory should contain both ./Alea and ./data-set directories. Files describing machines should be placed in a file named e.g., "metacentrum.mwf.machines".
        // Similarly machine failures (if simulated) should be placed in a file called e.g., "metacentrum.mwf.failures".
        // Please read carefully the copyright note when using public workload traces!
        //String data_sets[] = {"SDSC-SP2.swf", "hpc2n.swf", "star.swf", "thunder.swf", "metacentrum.mwf", "meta2008.mwf", "atlas.swf",};
        String data_sets[] = {"metacentrum.mwf"};

        // number of gridlets in data set
        //int total_gridlet[] = {59700, 202876, 96000, 121038, 103656, 187370, 42724,};
        int total_gridlet[] = {6, 6};
        // the weight of the fairness criteria in objective function
        int fairw[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

        // set true to use failures
        failures = false;
        // set true to use specific job requirements
        reqs = true;
        // set true to use runtime estimates
        estimates = false;

        // set true to refine estimates using job avg. length
        useAvgLength = false;
        // set true to use last job length as a new runtime estimate
        useLastLength = false;
        // set true to use "on demand" schedule optimization when early job completions appear
        useEventOpt = false;

        // set true to use very imprecise estimates
        useUserPrecision = false;
        // set true to use Feitelson's deterministic f-model to generate runtime estimates
        useDurationPrecision = false;
        // if useDurationPrecision = true, then following number denotes the level of imprecison
        // 100 = 2 x real lenght, 400 = 5 x real length, 900 = 10x, 1900 = 20x, 4900 =  50x
        userPercentage = 4900;
        // the minimal length (in seconds) of gap in schedule since when the "on demand" optimization is executed
        gap_length = 10 * 60;
        // the weigh of fairness criterion
        fair_weight = 1;

        // use binary heap dat structure to represent schedule (generally faster solution)
        useHeap = true;


        //defines the name format of output files
        String problem = "Result";
        if (!failures && !reqs) {
            problem += "Basic";
        }
        if (reqs) {
            problem += "R-";
        }
        if (failures) {
            problem += "F";
        }
        if (estimates) {
            problem += "-Estim";
        } else {
            problem += "-Exact";
        }
        if (useAvgLength) {
            problem += "-AvgL";
        }
        if (useLastLength) {
            problem += "-LastL";
        }
        if (useEventOpt) {
            problem += "-EventOpt";
        }
        if (useUserPrecision) {
            problem += "-UserPrec" + userPercentage;
        }
        if (useDurationPrecision) {
            problem += "-DurPrec" + userPercentage;
        }

        // data sets are outside the project folder (i.e. in ../data-set/)
        data = true;
        // multiply the number of iterations of optimization techniques
        multiplicator = 1;
        // used to influence the frequency of job arrivals (mwf files only)
        double multiplier = 1.0;

        // used only when executed on a real cluster (do not change)
        path = "estim100/";
        meta = false;
        if (meta) {
            String date = "-" + new Date().toString();
            date = date.replace(" ", "_");
            date = date.replace("CET_", "");
            date = date.replace(":", "-");
            System.out.println(date);
            problem += date;
        }

        String user_dir = "";
        if (ExperimentSetup.meta) {
            user_dir = "/scratch/xklusac/" + path;
        } else {
            user_dir = System.getProperty("user.dir");
        }
        try {
            Output out = new Output();
            out.deleteResults(user_dir + "/jobs(" + problem + "" + ExperimentSetup.algID + ").csv");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // creates Result Collector
        ResultCollector result_collector = new ResultCollector(results, problem);


        // this cycle selects data set from data_sets[] list
        for (int set = 0; set <= 0; set++) {
            String prob = problem;
            fair_weight = fairw[set];
            if (useUserPrecision) {
                prob += "-UserPrec" + userPercentage;
            }
            max_estim = 0;
            result_collector.generateHeader(data_sets[set] + "_" + prob);
            prevAlgID = -1;

            // selects algorithm
            // write down the IDs of algorithm that you want to use (FCFS = 0, EDF = 1, EASY = 2, CONS = 4, PBS PRO = 5, BestGap = 10, BestGap+RandomSearch = 11, ...)
            int algorithms[] = {2};

            // select which algorithms from the algorithms[] list will be used.
            for (int sel_alg = 0; sel_alg <= 0; sel_alg++) {

                // reset values from previous iterations
                use_compresion = false;
                opt_alg = null;
                fix_alg = null;

                // get proper algorithm
                int alg = algorithms[sel_alg];
                int experiment_count = 1;
                name = data_sets[set];
                algID = alg;
                if (sel_alg > 0) {
                    prevAlgID = algorithms[sel_alg - 1];
                }

                // used for output description
                String suff = "";
                // initialize the simulation - create the scheduler
                Scheduler scheduler = null;
                String scheduler_name = "Alea_3.0_scheduler";
                try {
                    Calendar calendar = Calendar.getInstance();
                    boolean trace_flag = false;  // true means tracing GridSim events
                    String[] exclude_from_file = {""};
                    String[] exclude_from_processing = {""};
                    String report_name = null;
                    GridSim.init(entities, calendar, trace_flag, exclude_from_file, exclude_from_processing, report_name);
                    scheduler = new Scheduler(scheduler_name, baudRate, entities, results, alg, data_sets[set], total_gridlet[set], suff, windows, result_collector, sel_alg);
                } catch (Exception ex) {
                    Logger.getLogger(ExperimentSetup.class.getName()).log(Level.SEVERE, null, ex);
                }
                // this will set up the proper algorithm according to the algorithms[] list
                if (alg == 0) {
                    policy = new FCFS(scheduler);
                    suff = "FCFS";
                }
                if (alg == 1) {
                    policy = new EDF(scheduler);
                    suff = "EDF";
                }
                if (alg == 2) {
                    policy = new EASY_Backfilling(scheduler);
                    // fixed version of EASY Backfilling
                    suff = "EASY";
                }
                if (alg == 4) {
                    policy = new CONS(scheduler);
                    use_compresion = true;
                    suff = "CONS+compression";
                }
                // do not use PBS-PRO on other than "metacentrum.mwf" data - not enough information is available.
                if (alg == 5) {
                    policy = new PBS_PRO(scheduler);
                    suff = "PBS-PRO";
                }

                if (alg == 10) {
                    policy = new BestGap(scheduler);
                    suff = "BestGap";
                }
                if (alg == 11) {
                    suff = "BestGap+RandSearch(" + multiplicator + ")";
                    policy = new BestGap(scheduler);
                    opt_alg = new RandomSearch();
                    if (useEventOpt) {
                        fix_alg = new GapSearch();
                        suff += "-EventOptLS";
                    }
                }

                if (alg == 19) {
                    suff = "CONS+LS(" + multiplicator + ")";
                    policy = new CONS(scheduler);
                    opt_alg = new GapSearch();

                    if (useEventOpt) {
                        fix_alg = new GapSearch();
                        suff += "-EventOptLS";
                    }
                }
                if (alg == 20) {
                    suff = "CONS+RandSearch(" + multiplicator + ")";
                    policy = new CONS(scheduler);
                    opt_alg = new RandomSearch();
                    if (useEventOpt) {
                        fix_alg = new GapSearch();
                        suff += "-EventOptLS";
                    }
                }
                if (alg == 21) {
                    suff = "CONS-no-compress";
                    policy = new CONS(scheduler);
                    if (useEventOpt) {
                        fix_alg = new GapSearch();
                        // instead of compression, use LS-based optimization on early job completion
                        suff += "-EventOptLS";
                    }
                }

                System.out.println("Now scheduling " + total_gridlet[set] + " jobs by: " + suff + ", using " + data_sets[set] + " data set.");

                suff += "@" + data_sets[set];

                // this cycle may be used when some modifications of one data set are required in multiple runs of Alea 3.0 over same data-set.
                for (int pass_count = 1; pass_count <= experiment_count; pass_count++) {

                    try {
                        // creates entities
                        String job_loader_name = data_sets[set] + "_JobLoader";
                        String failure_loader_name = data_sets[set] + "_FailureLoader";

                        // creates all grid resources
                        MachineLoader m_loader = new MachineLoader(10000, 3.0, data_sets[set]);
                        rnd_seed = sel_alg;

                        // creates 1 scheduler

                        JobLoader job_loader = new JobLoader(job_loader_name, baudRate, total_gridlet[set], data_sets[set], maxPE, minPErating, maxPErating,
                                multiplier, pass_count, m_loader.total_CPUs, estimates);
                        if (failures) {
                            FailureLoaderNew failure = new FailureLoaderNew(failure_loader_name, baudRate, data_sets[set], clusterNames, machineNames, 0);
                        }
                        // start the simulation
                        System.out.println("Starting the Alea 3.0");
                        GridSim.startGridSimulation();
                    } catch (Exception e) {
                        System.out.println("Unwanted errors happened!");
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                        System.out.println("Usage: java Test [time | space] [1-8]");
                    }

                    System.out.println("=============== END OF TEST " + pass_count + " ====================");
                    // reset inner variables of the simulator
                    Scheduler.load = 0.0;
                    Scheduler.classic_load = 0.0;
                    Scheduler.max_load = 0.0;
                    Scheduler.classic_activePEs = 0.0;
                    Scheduler.classic_availPEs = 0.0;
                    Scheduler.activePEs = 0.0;
                    Scheduler.availPEs = 0.0;
                    Scheduler.requestedPEs = 0.0;
                    Scheduler.last_event = 0.0;
                    Scheduler.start_event = -10.0;
                    Scheduler.runtime = 0.0;

                    // reset internal SimJava variables to start new experiment with different job/gridlet setup
                    Sim_system.setInComplete(true);
                    // store results
                    result_collector.generateResults(suff, experiment_count);
                    result_collector.reset();
                    results.clear();
                    System.out.println("Max. estim has been used = " + max_estim);
                    System.gc();
                }
            }
        }
        // end of the whole simulation
    }

    /** This method initializes the GUI, creating all windows that will be used to draw results. */
    private static void createGUI(LinkedList<Visualizator> windows) {

        LinkedList<Visualizator> ia = new LinkedList();
        Visualizator test1 = new Visualizator();
        JFrame f1 = new JFrame();
        f1.setTitle("Average system utilization");
        f1.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f1.getContentPane().add(test1);
        test1.setBackground(Color.white);
        f1.setSize(600, 310);
        f1.setLocation(130, 0);
        f1.setVisible(true);
        test1.setName("day_usage");
        test1.start();
        windows.add(test1);

        Visualizator test2 = new Visualizator();
        JFrame f2 = new JFrame();
        f2.setTitle("Average utilization per cluster");
        f2.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f2.getContentPane().add(test2);
        test2.setBackground(Color.white);
        f2.setSize(1410, 230);
        f2.setLocation(25, 630);
        f2.setVisible(true);
        test2.setName("cl_usage");
        test2.start();
        windows.add(test2);

        Visualizator test3 = new Visualizator();
        JFrame f3 = new JFrame();
        f3.setTitle("Waiting and running jobs");
        f3.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f3.getContentPane().add(test3);
        test3.setBackground(Color.white);
        f3.setSize(700, 310);
        f3.setLocation(735, 0);
        f3.setVisible(true);
        test3.setName("jobs");
        test3.start();
        windows.add(test3);

        Visualizator test4 = new Visualizator();
        JFrame f4 = new JFrame();
        f4.setTitle("Requested, available and used CPUs");
        f4.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f4.getContentPane().add(test4);
        test4.setBackground(Color.white);
        f4.setSize(700, 310);
        f4.setLocation(735, 315);
        f4.setVisible(true);
        test4.setName("CPUs");
        test4.start();
        windows.add(test4);

        Visualizator test5 = new Visualizator();
        JFrame f5 = new JFrame();
        f5.setTitle("24 hour usage profile");
        f5.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f5.getContentPane().add(test5);
        test5.setBackground(Color.white);
        f5.setSize(350, 310);
        f5.setLocation(25, 315);
        f5.setVisible(true);
        test5.setName("24hour_profile");
        test5.start();
        windows.add(test5);

        Visualizator test6 = new Visualizator();
        JFrame f6 = new JFrame();
        f6.setTitle("Cluster usage per hour");
        f6.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f6.getContentPane().add(test6);
        test6.setBackground(Color.white);
        f6.setSize(350, 310);
        f6.setLocation(380, 315);
        f6.setVisible(true);
        test6.setName("Cluster_hour_usage");
        test6.start();
        windows.add(test6);

        Visualizator test7 = new Visualizator();
        JFrame f7 = new JFrame();
        f7.setTitle("Average utilization per day");
        f7.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f7.getContentPane().add(test7);
        test7.setBackground(Color.white);
        f7.setSize(650, 310);
        f7.setLocation(200, 100);
        f7.setVisible(true);
        test7.setName("Cluster_day_usage");
        test7.start();
        windows.add(test7);

        Visualizator test8 = new Visualizator();
        JFrame f8 = new JFrame();
        f8.setTitle("Average UP/DOWN status per day");
        f8.setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
        //test.setOpaque(false);
        f8.getContentPane().add(test8);
        test8.setBackground(Color.white);
        f8.setSize(650, 310);
        f8.setLocation(320, 110);
        f8.setVisible(true);
        test8.setName("Cluster_day_status");
        test8.start();
        windows.add(test8);


        ia.add(test1);
        ia.add(test2);
        ia.add(test3);
        ia.add(test4);
        ia.add(test5);
        ia.add(test6);
        ia.add(test7);
        ia.add(test8);

        MainFrame mf = new MainFrame(ia);
        mf.setVisible(true);
    }
} // end class

