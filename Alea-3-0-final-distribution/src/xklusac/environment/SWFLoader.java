/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.environment;

/**
 *
 * @author Dalibor
 */
import eduni.simjava.Sim_event;
import gridsim.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import xklusac.extensions.*;
import eduni.simjava.distributions.Sim_normal_obj;

/**
 * Class SWFLoader<p>
 * Loads jobs dynamically over time from the file. Then sends these gridlets to the scheduler. SWF stands for Standard Workloads Format (SWF).
 * @author Dalibor Klusacek
 */
public class SWFLoader extends GridSim {

    /** input */
    Input r = new Input();
    /** current folder */
    String folder_prefix = "";
    /** buffered reader */
    BufferedReader br = null;
    /** total number of jobs in experiment */
    int total_jobs = 0;
    /** start time (for UNIX epoch converting) */
    int start_time = -1;
    /** message tag */
    private static int SendGridletInfo = 999;
    /** number of PEs in the "biggest" resource */
    int maxPE = 1;
    /** minimal PE rating of the slowest resource */
    int minPErating = 1;
    int maxPErating = 1;
    /** gridlet counter */
    int current_gl = 0;
    /** data set name */
    String data_set = "";
    /** counter of failed jobs (as stored in the GWF file) */
    int fail = 0;
    int help_j = 0;
    Random rander = new Random(4567);
    double last_delay = 0.0;
    Sim_normal_obj norm;

    /** Creates a new instance of JobLoader */
    public SWFLoader(String name, double baudRate, int total_jobs, String data_set, int maxPE, int minPErating, int maxPErating) throws Exception {
        super(name, baudRate);
        System.out.println("Openning all PWA blue jobs");
        if (ExperimentSetup.meta) {
            folder_prefix = "/scratch/xklusac/"+ExperimentSetup.path;
        } else {
            folder_prefix = System.getProperty("user.dir");
        }
        if(ExperimentSetup.data){
            String[] path = folder_prefix.split("/");
            if(path.length == 1){
                path = folder_prefix.split("\\\\");
            }
            folder_prefix = "";
            for(int i = 0; i < path.length - 1; i++){
            folder_prefix += path[i]+"/";
            }
            //System.out.println("Adresar = "+adresar);
        }
        br = r.openFile(new File(folder_prefix + "/data-set/" + data_set));
        this.total_jobs = total_jobs;
        this.maxPE = maxPE;
        this.minPErating = minPErating;
        this.maxPErating = maxPErating;
        this.data_set = data_set;
        this.norm = new Sim_normal_obj("normal distr", 0.0, 5.0, (121 + ExperimentSetup.rnd_seed));

    }

    /** Reads jobs from data_set file and sends them to the Scheduler entity dynamically over time. */
    public void body() {
        super.gridSimHold(10.0);    // hold by 10 second

        while (current_gl < total_jobs) {

            Sim_event ev = new Sim_event();
            sim_get_next(ev);

            if (ev.get_tag() == GridSimTags.JUNK_PKT) {

                ComplexGridlet gl = readGridlet(current_gl);
                current_gl++;
                if (gl == null && current_gl < total_jobs) {
                    super.sim_schedule(this.getEntityId(this.getEntityName()), 0.0, GridSimTags.JUNK_PKT);
                    continue;
                } else if (gl == null && current_gl >= total_jobs) {
                    continue;
                }
                // to synchronize job arrival wrt. the data set.
                double delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                // some time is needed to transfer this job to the scheduler, i.e., delay should be delay = delay - transfer_time. Fix this in the future.
                //System.out.println("Sending: "+gl.getGridletID());
                last_delay = delay;
                super.sim_schedule(this.getEntityId("Alea_3.0_scheduler"), delay, SendGridletInfo, gl);

                delay = Math.max(0.0, (gl.getArrival_time() - super.clock()));
                if (current_gl < total_jobs) {
                    // use delay - next job will be loaded after the simulation time is equal to the previous job arrival.
                    super.sim_schedule(this.getEntityId(this.getEntityName()), delay, GridSimTags.JUNK_PKT);
                }

                continue;
            }
        }
        System.out.println("Shuting down - last gridlet = " + current_gl + " of " + total_jobs);
        super.sim_schedule(this.getEntityId("Alea_3.0_scheduler"), Math.round(last_delay + 2), 612345, new Integer(current_gl));
        Sim_event ev = new Sim_event();
        sim_get_next(ev);

        if (ev.get_tag() == GridSimTags.END_OF_SIMULATION) {
            System.out.println("Shuting down the " + data_set + "_PWALoader... with: " + fail + " fails");
        }
        shutdownUserEntity();
        super.terminateIOEntities();


    }

    /** Reads one job from file. */
    private ComplexGridlet readGridlet(int j) {
        String[] values = null;
        String line = "";
        //System.out.println("Read job "+j);

        if (j == 0) {
            while (true) {
                try {
                    line = br.readLine();
                    values = line.split("\t");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                if (!values[0].contains(";")) {
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    if (line.charAt(0) == ' ') {
                        line = line.substring(1);
                    }
                    values = line.split("\\s+");
                    break;
                } else {
                    //System.out.println("error --- "+values[0]);
                }
            }
        } else {
            try {
                line = br.readLine();
                //System.out.println(">"+line+"<");
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                if (line.charAt(0) == ' ') {
                    line = line.substring(1);
                }
                //System.out.println("error1 = "+line+" at gi = "+j);
                values = line.split("\\s+");

            } catch (IOException ex) {
                System.out.println("error = " + values[0] + " at gi = " + j);
                ex.printStackTrace();
            }
        }
        // such line is not a job description - it is a typo in the SWF file
        if (values.length < 5 || values[1].equals("-1")) {
            fail++;
            System.out.println(j+" returning: null "+values[0]);
            return null;
        }



        // such job failed or was cancelled and no info about runtime or numCPU is available therefore we skip it
        if (values[3].equals("-1") || values[4].equals("-1")) {
            fail++;
            //System.out.println("returning: null2 ");
            return null;
        }
        //System.out.println(values[0]+"+"+values[1]+"+"+values[2] + ": Number parsing error: " + values[4]);
        int id = Integer.parseInt(values[0]);
        int numCPU;
        try {
            numCPU = Integer.parseInt(values[4]);
        } catch (NumberFormatException ex) {
            System.out.println(values[0] + ": Number parsing error: " + values[4]);
            //ex.printStackTrace();
            numCPU = 1;
        }

        // we do not allow more PEs for one job than there is on the "biggest" machine.
        // Co-allocation is only supported over one cluster (GridResource) by now.
        if (numCPU > maxPE) {
            numCPU = maxPE;

        }


        long arrival = 0;
        // synchronize GridSim's arrivals with the UNIX epoch format as given in GWF
        if (start_time < 0) {
            //System.out.println("prvni: "+j+" start at:"+values[1]+" line="+line);
            start_time = Integer.parseInt(values[1]);
            arrival = 0;

        } else {

            arrival = ((Integer.parseInt(values[1]) - start_time));
        //System.out.println("pokracujeme..."+arrival);

        }

        // minPErating is the default speed of the slowest machine in the data set        
        double length = Math.round((Integer.parseInt(values[3])) * maxPErating);

        // manually established - fix it according to your needs
        double deadline = arrival + Integer.parseInt(values[3]) * 2;

        // queue name
        String queue = "q3";
        String properties = "";
        // finally create gridlet
        //numCPU = 1;
        long job_limit = Integer.parseInt(values[8]);
        if (job_limit < 0) {
            // atlas = 432000
            // thunder = 432000
            if (data_set.equals("thunder.swf")){
                job_limit = 48000; //13 hours 20 min
                ExperimentSetup.max_estim++;
            } else if( data_set.equals("atlas.swf")){
                job_limit = 73200; //20 hours 20 minutes
                ExperimentSetup.max_estim++;
            } else if (data_set.equals("star.swf")){
                job_limit = 64800; //18 hours
                ExperimentSetup.max_estim++;                
            }else{
                job_limit = Integer.parseInt(values[3]);
            }
        }

        double estimatedLength = 0.0;
        if (ExperimentSetup.estimates) {
            //roughest estimate that can be done = queue limit        
            estimatedLength = Math.round(Math.max((job_limit * maxPErating), length));
        //System.out.println(id+" Estimates "+estimatedLength+" real = "+length);
        } else {
            // exact estimates
            estimatedLength = length;
        //System.out.println(id+" Exact "+estimatedLength);
        }
        
        //double perc = Math.min(100.0, (norm.sample()+ExperimentSetup.userPercentage));       
        double perc = norm.sample()+ExperimentSetup.userPercentage;       

        ComplexGridlet gl = new ComplexGridlet(id, values[11], job_limit, new Double(length), estimatedLength, 10, 10, "Linux", "Risc arch.", arrival, deadline, 1, numCPU, 0.0, queue, properties, perc);

        // and set user id to the Scheduler entity - otherwise it would be returned to the JobLoader when completed.
        System.out.println(id+" job has length = "+gl.getGridletLength());
        gl.setUserID(super.getEntityId("Alea_3.0_scheduler"));
        return gl;
    }
}
