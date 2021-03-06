package xklusac.environment;

import gridsim.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import xklusac.extensions.BinaryHeap;
import xklusac.extensions.HeapNode;
import xklusac.extensions.Hole;
import xklusac.extensions.StartComparator;

/**
 * Class ResourceInfo<p>
 * This class stores dynamic information about each resource. E.g., prepared schedule for this resource,
 * list of gridletDescriptions of jobs in execution/waiting on machine. It also provides methods to calculate various parameters based on
 * the knowledge of the schedule/queue and resource status, e.g. expected makespan, machine usage, first free slot, etc.
 *
 * @author       Dalibor Klusacek
 */
public class ResourceInfo {

    /** Resource char. object - used to get information about Resource*/
    public ComplexResourceCharacteristics resource;
    /** Denotes the total number of PE on Resource */
    public int numPE;
    /** List of gridletInfos "on Resource" */
    public ArrayList<GridletInfo> resInExec;
    /** List representing schedule for this resource (gridletInfos) */
    public ArrayList<GridletInfo> resSchedule;
    /** Sum of tardiness of all finished jobs */
    protected double prev_tard = 0.0;
    /** Denotes if previously selected gridlet was succesfully sended by JSS - prevents anticipating of gridlets */
    public boolean is_ready = true;
    /** denotes previous deadline score of already finished jobs */
    protected int prev_score = 0;
    /** current tardiness of jobs */
    public double resource_tardiness = 0.0;
    /** current makespan */
    public double resource_makespan = 0.0;
    /** number of nondelayed jobs so far */
    public int resource_score = 0;
    /** number of nondelayed jobs in schedule */
    public int expected_score = 0;
    /** number of delayed jobs in schedule */
    public int expected_fails = 0;
    /** stable == true means that currently stored resource-related information are correct and up-to-date */
    protected boolean stable = false;
    /** previous time when update was performed */
    protected double prev_clock = 0.0;
    /** when will be PEs free */
    protected double finishTimeOnPE[] = null;
    /** earliest start time (queue only) */
    public double est = Double.MAX_VALUE - 10;
    public int usablePEs = 0;
    /** list of holes (gaps) in schedule */
    protected LinkedList holes = new LinkedList();
    /** total lenght of holes */
    protected double holes_length = 0.0;
    /** total MIPS as available in holes */
    protected double holes_mips = 0.0;
    /** actual resource usage */
    public double res_usage = 0.0;
    /** avg start time */
    public double accum_start_time = 0.0;
    /** average slowdown */
    public double accum_sd = 0.0;
    /** average wait time */
    public double accum_wait = 0.0;
    /** average response time */
    public double accum_resp = 0.0;
    /** This resource's PE rating */
    public int peRating = 0;
    protected boolean stable_w = false;
    protected boolean stable_s = false;
    double[] r_tuwt;
    double[] r_tusa;

    /** Creates a new instance of ResourceInfo with "in schedule" and "on resource" lists of gridletInfos
     *@param resource Resource characteristics (number of CPU, rating, etc.)
     */
    public ResourceInfo(ComplexResourceCharacteristics resource) {
        this.resource = resource;
        this.numPE = resource.getNumPE();
        this.finishTimeOnPE = new double[resource.getNumPE()];
        this.resInExec = new ArrayList();
        this.resSchedule = new ArrayList();
        this.peRating = resource.getMIPSRatingOfOnePE();
        this.stable_w = false;
        this.stable_s = false;
    }

    /** Removes GridletInfo from list of "Gridlets on Resource" (only GridletInfo there not gridlets)
     *@param gi gridletInfo to be removed
     */
    public void lowerResInExec(GridletInfo gi) {
        boolean removed = false;
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo giRes = (GridletInfo) resInExec.get(j);

            if (giRes.getID() == gi.getID() && giRes.getOwnerID() == gi.getOwnerID()) {
                resInExec.remove(j);
                stable = false;
                stable_w = false;
                stable_s = false;
                removed = true;
                break;
            }
        }
        if (!removed) {
            System.out.println("Error removing gi from InExec list.");
        }
    }

    /** Removes GridletInfo from Resource schedule
     *@param gi gridletInfo to be removed
     */
    public void lowerResScheduleList(GridletInfo gi) {
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo giRes = (GridletInfo) resSchedule.get(j);
            if (giRes.getID() == gi.getID() && giRes.getOwnerID() == gi.getOwnerID()) {
                resSchedule.remove(j);
                stable = false;
                stable_w = false;
                stable_s = false;
                break;
            }
        }
    }

    /*
     * Gets the number of currently free CPUs on a resource.
     */
    public int getNumFreePE() {
        //int freePE = this.numPE;
        int freePE = getNumRunningPE();
        // just testing
        freePE = Math.min(freePE, this.numPE);
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            if (gi.getStatus() != Gridlet.SUCCESS) {
                freePE = freePE - gi.getNumPE();
            }
        }
        return Math.max(0, freePE);
    }

     public int getNumFreeNodes()
    {
        int freeNodes = resource.getMachineList().size();
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            if (gi.getStatus() != Gridlet.SUCCESS) {
                freeNodes = freeNodes - gi.getGridlet().getNumNodes();
            }
        }
        return Math.max(0,freeNodes);
        
    }
   
    
    /*
     * Gets the number of currently working (not failed) CPUs on a resource.
     */

    public int getNumRunningPE() {
        int running = 0;

        if (ExperimentSetup.failures) {
            MachineList mlist = this.resource.getMachineList();

            for (int i = 0; i < mlist.size(); i++) {
                Machine m = mlist.getMachine(i);
                if (m.getFailed() == false) {
                    running += m.getNumPE();
                }
            }
            return running;
        } else {
            return this.resource.getNumPE();
        }
    }
    /*
     * Gets the number of currently busy CPUs on a resource.
     */

    public int getNumBusyPE() {
        int busy = 0;
        MachineList mlist = this.resource.getMachineList();

        for (int i = 0; i < mlist.size(); i++) {
            Machine m = mlist.getMachine(i);
            if (m.getFailed() == false) {
                busy += m.getNumBusyPE();
            }
        }
        return busy;
    }

    /** Updates start times in the "CPU field" according to release date of multi-CPU gridlet - auxiliary method
     *@param finishTimeOnPE[] field representing earliest free slot of each CPU on machine
     *@param start_time either current time or release date - according to what is higher
     */
    private void updateMultiStartTime(double finishTimeOnPE[], double start_time) {

        for (int j = 0; j < finishTimeOnPE.length; j++) {
            if (finishTimeOnPE[j] < start_time && finishTimeOnPE[j] > -998.0) {
                finishTimeOnPE[j] = start_time;
            }
        }
    }

    /** Selects index of the last CPU necessary to run multi-CPU gridlet. Auxiliary method
     * @param finishTimeOnPE[] field representing earliest free slot of each CPU on machine
     * @param gi gridletInfo describing the multi-CPU gridlet
     */
    private int findFirstFreeSlot(double finishTimeOnPE[], GridletInfo gi) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        //gi.getPEs().clear();

        for (int i = 0; i < gi.getNumPE(); i++) {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                    min = finishTimeOnPE[j];
                    index = j;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (gi.getNumPE() - 1)) {
                //gi.getPEs().add(index);
                finishTimeOnPE[index] = -999;
            }
        }
        //gi.getPEs().add(index);
        //System.out.println("time = "+finishTimeOnPE[index]);
        return index;
    }

    /** Auxiliary method for EASY Backfilling */
    private int findUsablePEs(int index, double finishTimeOnPE[], GridletInfo gi) {

        int usable = 0;
        double earliest = 0.0;

        earliest = finishTimeOnPE[index];
        finishTimeOnPE[index] = -999;

        // count usable
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            // if other PE needed to run gridlet - be carefull when comparing 2 double values
            if (finishTimeOnPE[j] == earliest) {
                usable++;
            }
        }
        return usable;
    }

    /*
     * Auxiliary function - predicts when and which PEs will be used for this gi.
     *
     */
    private void predictPEs(double finishTimeOnPE[], GridletInfo gi) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        LinkedList<Integer> PEs = new LinkedList();

        for (int i = 0; i < gi.getNumPE(); i++) {
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                    min = finishTimeOnPE[j];
                    index = j;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (gi.getNumPE() - 1)) {
                PEs.add(index);
                finishTimeOnPE[index] = -999;
            }
        }
        PEs.add(index);
        gi.setPEs(PEs);
    }

    /*
     * Auxiliary function for correct Hole (Gap) creation. Generates gaps at the end of schedule.
     *
     */
    private int createLastGaps(double finishTimeOnPE2[], int numPE) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        double hole_start = -1.0;
        int hole_size = 1;
        //gi.getPEs().clear();
        //System.out.println("finding gaps over = "+numPE);


        for (int i = 0; i < numPE; i++) {
            for (int j = 0; j < finishTimeOnPE2.length; j++) {
                // if other PE needed to run gridlet - be carefull when comparing 2 double values
                if (finishTimeOnPE2[j] < min && finishTimeOnPE2[j] > -998) {
                    min = finishTimeOnPE2[j];
                    index = j;
                }
            }
            if (hole_start <= 0.0) { // hole not started yet

                hole_start = min; // possible hole start

            } else { // finish hole or continue creating it?

                if (hole_start != min) {
                    // we have a new hole in schedule - store it.
                    double length = min - hole_start;
                    Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, null);
                    holes_length += length * hole_size;
                    holes_mips += length * peRating * hole_size;
                    holes.addLast(h);
                    hole_size++;
                    hole_start = min;
                } else {
                    hole_size++;
                }
            }

            //reset min value if not the last PE allocated
            min = Double.MAX_VALUE - 10; //here remember hole

            if (i != (numPE - 1)) {
                //gi.getPEs().add(index);
                finishTimeOnPE2[index] = -999;
            }
        }

        Hole h_last = new Hole(finishTimeOnPE2[index], Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                numPE, null);
        holes.addLast(h_last);
        //gi.getPEs().add(index);
        return index;
    }

    /** This method creates last gaps at the end of schedule */
    private void createLastGapsFast(double finishTimeOnPE2[], int numPE, double min, double max) {
        int hole_size = 0;

        for (int j = 0; j < finishTimeOnPE2.length; j++) {
            if (finishTimeOnPE2[j] <= min) {
                hole_size++;
            }
        }
        double length = max - min;
        if (length > 0.0) {
            Hole h = new Hole(min, max, length, (length * peRating), hole_size, null);
            holes_length += length * hole_size;
            holes_mips += length * peRating * hole_size;
            holes.addLast(h);
        }
        Hole h_last = new Hole(max, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                numPE, null);
        holes.addLast(h_last);
    }

    /** This method constructs the binary heap that stores information about first free slots on CPUs. */
    private BinaryHeap createBinaryHeap() {
        BinaryHeap slots = new BinaryHeap();
        HashMap<Double, ArrayList> time_slots = new HashMap<Double, ArrayList>();
        double time = 0.0;

        for (int i = 0; i < finishTimeOnPE.length; i++) {
            time = finishTimeOnPE[i];
            if (!time_slots.containsKey(time)) {
                ArrayList<Integer> cpus = new ArrayList<Integer>();
                cpus.add(i);
                time_slots.put(time, cpus);
            } else {
                ArrayList<Integer> cpus = time_slots.get(time);
                cpus.add(i);
                time_slots.put(time, cpus);
            }
        }
        Object[] keys = time_slots.keySet().toArray();
        for (int i = 0; i < time_slots.size(); i++) {
            ArrayList<Integer> cpus = time_slots.get((Double) keys[i]);
            slots.insert(new HeapNode((Double) keys[i], cpus));
        }
        return slots;
    }

    /** Selects index of the last CPU necessary to run multi-CPU gridlet. Also, it builds the hole-list during execution. Auxiliary method.
     * @param finishTimeOnPE[] field representing earliest free slot of each CPU on machine
     * @param gi gridletInfo describing the multi-CPU gridlet
     * @param slots Binary heap with first free slots
     */
    private int findFirstFreeSlotForWaitingJob(double finishTimeOnPE[], GridletInfo gi, BinaryHeap slots) {
        int index = 0;
        double min = Double.MAX_VALUE - 10;
        double hole_start = -1.0;
        int hole_size = 1;
        double earl_job_start = 0.0;
        //gi.getPEs().clear();
        //if(gi.getID()>4000)
        //System.out.println(gi.getID()+": update on res: "+this.resource.getResourceName()+" Heap size = "+slots.size()+" CPUS = "+slots.getCPUcount());
        //slots.printCPUcount("start for "+gi.getID());
        if (ExperimentSetup.useHeap) {
            int needed = gi.getNumPE();
            int found = 0;
            hole_size = 0;
            ArrayList<Integer> usedIDs = new ArrayList<Integer>();
            while (needed > 0) {

                //System.out.println(gi.getID()+":  needed = "+needed);
                HeapNode hn = (HeapNode) slots.findMin();
                earl_job_start = hn.getTime();
                ArrayList<Integer> cpuIDs = hn.getCpuIDs(gi.getID());
                // all ids in this node
                if (cpuIDs.size() >= needed) {
                    //System.out.println("prior size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    for (int i = 0; i < needed; i++) {
                        usedIDs.add(cpuIDs.remove(0));
                        //System.out.println(i+": size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    }
                    //System.out.println("post size = "+cpuIDs.size()+" used = "+usedIDs.size());
                    needed = 0;
                    // vyprazdneny uzel odstranime
                    if (cpuIDs.size() == 0) {
                        //hn.setCpuIDs(cpuIDs);
                        slots.deleteMin();
                    } else {
                        hn.setCpuIDs(cpuIDs);
                    }
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": OK needed = "+needed+" of "+gi.getNumPE()+" Res_cpus = "+finishTimeOnPE.length+" heap size = "+slots.size()+" so far = "+usedIDs.size());
                    //slots.printCPUcount("all found for "+gi.getID());

                    // more nodes needed => empty the whole node while gap will appear as a side effect
                } else {
                    int founded = cpuIDs.size();
                    needed = needed - founded;
                    hole_size += founded;
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": prior "+founded+" steps have "+usedIDs.size());
                    for (int i = 0; i < founded; i++) {

                        usedIDs.add(cpuIDs.remove(0));
                        //if(gi.getID()>4000) System.out.println(gi.getID()+": in "+i+" th step have "+usedIDs.size());
                    }
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": past "+founded+" steps have "+usedIDs.size());
                    //hn.setCpuIDs(cpuIDs);
                    hole_start = hn.getTime();
                    // delete empty node
                    slots.deleteMin();
                    //create hole
                    //if(gi.getID()>4000) System.out.println(gi.getID()+": KO needed = "+needed+" of "+gi.getNumPE()+" Res_cpus = "+finishTimeOnPE.length+" heap size = "+slots.size()+" so far = "+usedIDs.size());
                    //slots.printCPUcount("more needed for "+gi.getID());
                    HeapNode hnext = (HeapNode) slots.findMin();
                    min = hnext.getTime();
                    double length = min - hole_start;
                    Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, gi);
                    //System.out.println(gi.getID()+" New hole s = "+Math.round(hole_start)+" size = "+hole_size+" length = "+Math.round(length));
                    holes_length += length * hole_size;
                    holes_mips += length * peRating * hole_size;
                    holes.addLast(h);
                }
            }
            // update of binary heap structure
            double glFinishTime = gi.getJobRuntime(peRating);
            if (glFinishTime < 1.0) {
                glFinishTime = 1.0;
            }
            int roundUpTime = (int) (glFinishTime + 1);
            double end = earl_job_start + roundUpTime;
            //if(gi.getID()>4000) System.out.println(gi.getID()+": Insert new node with CPUids "+usedIDs.size()+" == "+gi.getNumPE());
            slots.insert(new HeapNode(end, usedIDs));
            // update of old structure - compatibility reasons
            int lasti = usedIDs.get(usedIDs.size() - 1);



            for (int i = 0; i < usedIDs.size(); i++) {
                index = usedIDs.get(i);
                if (i != (usedIDs.size() - 1)) {
                    finishTimeOnPE[index] = -999;
                }

            }
            index = lasti;

        } else {
            // classical array will be used instead of Binary Heap
            for (int i = 0; i < gi.getNumPE(); i++) {
                for (int j = 0; j < finishTimeOnPE.length; j++) {
                    // if other PE needed to run gridlet - be carefull when comparing 2 double values
                    if (finishTimeOnPE[j] < min && finishTimeOnPE[j] > -998) {
                        min = finishTimeOnPE[j];
                        index = j;
                    }
                }
                if (hole_start <= 0.0) { // hole not started yet

                    hole_start = min; // possible hole start

                } else { // finish hole or continue creating it?

                    if (hole_start != min) {
                        // we have a new hole in schedule - store it.
                        double length = min - hole_start;
                        Hole h = new Hole(hole_start, min, length, (length * peRating), hole_size, gi);
                        holes_length += length * hole_size;
                        holes_mips += length * peRating * hole_size;
                        holes.addLast(h);
                        hole_size++;
                        hole_start = min;
                    } else {
                        hole_size++;
                    }
                }

                //reset min value if not the last PE allocated
                min = Double.MAX_VALUE - 10; //here remember hole

                if (i != (gi.getNumPE() - 1)) {
                    //gi.getPEs().add(index);
                    finishTimeOnPE[index] = -999;
                }
            }
            //gi.getPEs().add(index);
        }
        return index;
    }
    /*
     * Auxiliary function predicting completion time of running jobs.
     *
     */

    private void predictFirstFreeSlots(double current_time) {
        int peIndex = 0;

        // first - failed machines must have finishTimeOnPE[peIndex] = MAX_VALUE
        if (ExperimentSetup.failures) {
            MachineList list = resource.getMachineList();
            for (int i = 0; i < list.size(); i++) {
                Machine m = (Machine) list.get(i);
                if (!m.getFailed()) {
                    //numPE += m.getNumPE();
                    for (int p = 0; p < m.getNumPE(); p++) {
                        //System.out.print(index_id+".");
                        finishTimeOnPE[peIndex] = current_time;
                        peIndex++;
                    }
                } else {
                    for (int p = 0; p < m.getNumPE(); p++) {
                        //System.out.print(index_id+".");
                        finishTimeOnPE[peIndex] = Double.MAX_VALUE;
                        peIndex++;
                    }
                }

            }
        } else {
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                finishTimeOnPE[i] = current_time;
            }
        }
        peIndex = 0;

        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            LinkedList<Integer> PEs = gi.getPEs();
            if (gi.getStatus() == Gridlet.INEXEC) {
                double run_time = current_time - gi.getGridlet().getExecStartTime();
                double time_remaining = Math.max(0.0, (gi.getJobRuntime(peRating) - run_time));
                // update all PE-finish-time that will run this gridlet
                for (int k = 0; k < gi.getNumPE(); k++) {
                    finishTimeOnPE[PEs.get(k)] += time_remaining;
                    peIndex++;
                }

                double giTard = Math.max(0.0, finishTimeOnPE[PEs.get(0)] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[PEs.get(0)]);
                gi.setTardiness(giTard);
            } else if (gi.getStatus() != Gridlet.SUCCESS && gi.getStatus() != Gridlet.INEXEC && gi.getStatus() != Gridlet.QUEUED && gi.getStatus() != Gridlet.FAILED_RESOURCE_UNAVAILABLE) {
                //System.out.println(gi.getID() + " status=" + gi.getGridlet().getGridletStatusString() + " resource=" + resource.getResourceName()+" at clock="+GridSim.clock());
                if (PEs.size() < gi.getNumPE()) {
                    predictPEs(finishTimeOnPE, gi);
                }
                double max = 0.0;
                PEs = gi.getPEs();
                for (int k = 0; k < gi.getNumPE(); k++) {
                    if (max < finishTimeOnPE[PEs.get(k)]) {
                        max = finishTimeOnPE[PEs.get(k)];
                    }
                }
                for (int k = 0; k < gi.getNumPE(); k++) {
                    finishTimeOnPE[PEs.get(k)] = max + gi.getJobRuntime(peRating);
                }
                double giTard = Math.max(0.0, finishTimeOnPE[PEs.get(0)] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[PEs.get(0)]);
                gi.setTardiness(giTard);
            } else if (gi.getStatus() == Gridlet.QUEUED) {
                // this if branch should not really execute. If so, something is wrong.
                System.out.println("++++++++++++++++++++++++ QUEUED");
                // return the last needed PE index (others finish-time set to -999)
                peIndex = findFirstFreeSlot(finishTimeOnPE, gi);
                finishTimeOnPE[peIndex] += gi.getJobRuntime(peRating);
                double giTard = Math.max(0.0, finishTimeOnPE[peIndex] - gi.getDue_date());
                gi.setExpectedFinishTime(finishTimeOnPE[peIndex]);
                gi.setTardiness(giTard);
                // update all PE-finish-time that will run this gridlet
                for (int k = 0; k < finishTimeOnPE.length; k++) {
                    if (finishTimeOnPE[k] < -998) {
                        finishTimeOnPE[k] = finishTimeOnPE[peIndex];
                    }
                }
            }
        }
    }

    /**
     * This method update information about schedule such as job start/finish time, number of nondelayed jobs, makespan, etc. Usefull for schedule-based methods mainly.
     * It can be easily modified to provide more information about e.g., slowdown. If there is no change since the last computation
     * it is not performed to save time.
     *
     */
    public void update(double current_time) {
        double total_tardiness = 0.0;
        double tardiness = 0.0;
        int nondelayed = 0;
        double start_hole_min = Double.MAX_VALUE;
        double start_hole_max = 0.0;
        double end_hole = 0.0;
        int size_hole = 0;
        GridletInfo last = null;
        int idUns[] = new int[resSchedule.size()];


        if (prev_clock == current_time && stable) {
            // no change - so save computational time            
            return;
        } else {
            stable_w = false;
            stable_s = false;
            // setup the field representing CPUs earliest free slot times
            holes.clear();
            holes_length = 0.0;
            holes_mips = 0.0;
            res_usage = 0.0;
            accum_start_time = 0.0;
            accum_sd = 0.0;
            accum_wait = 0.0;
            accum_resp = 0.0;

            // initialize the free slot array (must be done)
            predictFirstFreeSlots(current_time); //OK works
            BinaryHeap slots = null;
            if (ExperimentSetup.useHeap) {
                slots = createBinaryHeap();
            }

            // calculate all required values for jobs in schedule

            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);
                idUns[j] = gi.getID();
                //update the res_usage value
                res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();

                // simulate the FCFS attitude of LRM on the resource
                int index = findFirstFreeSlotForWaitingJob(finishTimeOnPE, gi, slots);
                gi.setInit(false);
                // set expected start time wrt. current schedule
                gi.setExpectedStartTime(finishTimeOnPE[index]);
                accum_start_time += finishTimeOnPE[index];


                double glFinishTime = gi.getJobRuntime(peRating);
                if (glFinishTime < 1.0) {
                    glFinishTime = 1.0;
                }
                int roundUpTime = (int) (glFinishTime + 1);

                double earliestNextTime = finishTimeOnPE[index];

                // time when the gridlet will be probably finished on CPU #index
                finishTimeOnPE[index] += roundUpTime;
                // sets expected finish time
                gi.setExpectedFinishTime(finishTimeOnPE[index]);
                // tardiness of this gridlet in this schedule
                tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
                accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
                accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
                accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

                gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

                if (tardiness <= 0.0) {
                    nondelayed++;
                }
                total_tardiness += tardiness;

                // update also the rest of PEs finish-time required to run this gridlet
                for (int k = 0; k < finishTimeOnPE.length; k++) {
                    if (finishTimeOnPE[k] < -998) {
                        finishTimeOnPE[k] = finishTimeOnPE[index];
                    }
                }
                //start_hole = earliestNextTime;
                start_hole_max = finishTimeOnPE[index];
            }


            // prepare min and max starting points for last gap
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                if (finishTimeOnPE[i] > start_hole_max) {
                    start_hole_max = finishTimeOnPE[i];
                }
                if (finishTimeOnPE[i] < start_hole_min) {
                    start_hole_min = finishTimeOnPE[i];
                }
            }

            expected_fails = resSchedule.size() - nondelayed;

            // add expected tardiness of running jobs
            for (int j = 0; j < resInExec.size(); j++) {
                GridletInfo gi = (GridletInfo) resInExec.get(j);
                total_tardiness += gi.getTardiness();
                if (gi.getTardiness() <= 0.0) {
                    nondelayed++;
                }
            }

            // calculate makespan
            double makespan = 0.0;
            for (int j = 0; j < finishTimeOnPE.length; j++) {
                if (finishTimeOnPE[j] > makespan) {
                    makespan = finishTimeOnPE[j];
                }
            }

            // add tardiness and score of already finished jobs
            expected_score = nondelayed;
            total_tardiness += prev_tard;
            nondelayed += prev_score;

            // set the variables to new values
            resource_tardiness = total_tardiness;
            resource_score = nondelayed;
            resource_makespan = makespan;

            stable = true;
            prev_clock = current_time;

            //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
            //holes.addLast(h_last);
            //System.arraycopy(est, numPE, est, numPE, numPE);

            double finishTimeOnPE2[] = new double[resource.getNumPE()];
            for (int i = 0; i < finishTimeOnPE.length; i++) {
                finishTimeOnPE2[i] = finishTimeOnPE[i];
            }
            // add hole to the end of schedule (infinite hole)
            //createLastHoles(finishTimeOnPE2, numPE);
            createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);

            //sort the schedule via start times - so that less gaps will appear
            Collections.sort(resSchedule, new StartComparator());

            for (int j = 0; j < resSchedule.size(); j++) {
                GridletInfo gi = (GridletInfo) resSchedule.get(j);
                if (gi.getID() != idUns[j]) {
                    System.out.println("Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
                }
            }

        }
    }

    /**
     * This method updates all information about the schedule even when no change appears - more time overhead.
     * The functionality is the same as in case of update(current_time);
     *
     */
    public void forceUpdate(double current_time) {
        double total_tardiness = 0.0;
        double tardiness = 0.0;
        int nondelayed = 0;
        double start_hole_max = 0.0;
        double start_hole_min = Double.MAX_VALUE;
        GridletInfo last = null;
        // setup the field representing CPUs earliest free slot times
        holes.clear();
        holes_length = 0.0;
        holes_mips = 0.0;
        res_usage = 0.0;
        accum_start_time = 0.0;
        accum_sd = 0.0;
        accum_wait = 0.0;
        accum_resp = 0.0;
        int idUns[] = new int[resSchedule.size()];
        stable_w = false;
        stable_s = false;

        // initialize the free slot array (must be done)
        predictFirstFreeSlots(current_time); //OK works
        BinaryHeap slots = null;
        if (ExperimentSetup.useHeap) {
            slots = createBinaryHeap();
        }

        // calculate all required values for jobs in schedule
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            idUns[j] = gi.getID();
            //update the res_usage value
            res_usage += gi.getJobRuntime(peRating) * peRating * gi.getNumPE();

            // simulate the FCFS attitude of LRM on the resource
            int index = findFirstFreeSlotForWaitingJob(finishTimeOnPE, gi, slots);
            gi.setInit(false);
            gi.setExpectedStartTime(finishTimeOnPE[index]);
            accum_start_time += finishTimeOnPE[index];


            double glFinishTime = gi.getJobRuntime(peRating);
            if (glFinishTime < 1.0) {
                glFinishTime = 1.0;
            }
            int roundUpTime = (int) (glFinishTime + 1);

            double earliestNextTime = finishTimeOnPE[index];

            // time when the gridlet will be probably finished on CPU #index
            finishTimeOnPE[index] += roundUpTime;
            gi.setExpectedFinishTime(finishTimeOnPE[index]);

            // tardiness of this gridlet in this schedule
            tardiness = Math.max(0.0, finishTimeOnPE[index] - gi.getDue_date());
            accum_wait += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
            accum_resp += Math.max(0.0, gi.getExpectedFinishTime() - gi.getRelease_date());
            accum_sd += (Math.max(1.0, (finishTimeOnPE[index] - gi.getRelease_date()))) / Math.max(1.0, roundUpTime);

            gi.setTardiness(tardiness); // after this method we know each gridlet's tardiness

            if (tardiness <= 0.0) {
                nondelayed++;
            }
            total_tardiness += tardiness;

            // update also the rest of PEs finish-time required to run this gridlet
            for (int k = 0; k < finishTimeOnPE.length; k++) {
                if (finishTimeOnPE[k] < -998) {
                    finishTimeOnPE[k] = finishTimeOnPE[index];
                }
                /*else if(finishTimeOnPE[k] < earliestNextTime){
                // since it is FCFS resource, do no allow earlier starts
                finishTimeOnPE[k] = earliestNextTime;
                }*/
            }
            start_hole_max = earliestNextTime;
        }
        // prepare min and max starting points for last gap
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            if (finishTimeOnPE[i] > start_hole_max) {
                start_hole_max = finishTimeOnPE[i];
            }
            if (finishTimeOnPE[i] < start_hole_min) {
                start_hole_min = finishTimeOnPE[i];
            }
        }


        expected_fails = resSchedule.size() - nondelayed;

        // add expected tardiness of running jobs
        for (int j = 0; j < resInExec.size(); j++) {
            GridletInfo gi = (GridletInfo) resInExec.get(j);
            total_tardiness += gi.getTardiness();
            if (gi.getTardiness() <= 0.0) {
                nondelayed++;
            }
        }

        double makespan = 0.0;
        for (int j = 0; j < finishTimeOnPE.length; j++) {
            if (finishTimeOnPE[j] > makespan) {
                makespan = finishTimeOnPE[j];
            }
        }

        // add tardiness and score of already finished jobs
        expected_score = nondelayed;
        total_tardiness += prev_tard;
        nondelayed += prev_score;

        // set the variables to new values
        resource_tardiness = total_tardiness;
        resource_score = nondelayed;
        resource_makespan = makespan;

        stable = true;
        prev_clock = current_time;
        //Hole h_last = new Hole(start_hole, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, numPE, last);
        //holes.addLast(h_last);

        double finishTimeOnPE2[] = new double[resource.getNumPE()];
        for (int i = 0; i < finishTimeOnPE.length; i++) {
            finishTimeOnPE2[i] = finishTimeOnPE[i];
        }
        // add hole to the end of schedule (infinite hole)
        //createLastHoles(finishTimeOnPE2, numPE);
        createLastGapsFast(finishTimeOnPE2, numPE, start_hole_min, start_hole_max);

        //sort the schedule via start times - so that less gaps will appear
        Collections.sort(resSchedule, new StartComparator());
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            if (gi.getID() != idUns[j]) {
                System.out.println("Force: Sorted, gaps corrupted..." + gi.getID() + "/" + idUns[j]);
            }
        }
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public boolean removeGInfo(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        holes.clear();
        return resSchedule.remove(gi);
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public GridletInfo removeGInfoIndex(int index) {
        stable = false;
        stable_w = false;
        stable_s = false;
        holes.clear();
        return (GridletInfo) resSchedule.remove(index);
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public GridletInfo removeFirstGI() {
        stable = false;
        stable_w = false;
        stable_s = false;
        GridletInfo gi = (GridletInfo) resSchedule.remove(0);
        holes.clear();
        return gi;
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public void addLastGInfo(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        resSchedule.add(gi);
        gi.getPEs().clear();
        holes.clear();
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public void addGInfo(int index, GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        resSchedule.add(index, gi);
        gi.getPEs().clear();
        holes.clear();
    }

    /**
     * Auxiliary method - once schedule is changed it is not stable until update method is called
     */
    public void addGInfoInExec(GridletInfo gi) {
        stable = false;
        stable_w = false;
        stable_s = false;
        resInExec.add(gi);
        holes.clear();

    }

    /**
     * This method force recomputation of jobs-on-resource status. It also updates information about their
     * expected finish time, tardiness etc.
     */
    public void updateFinishTimeOfAssignedGridlets(double current_time) {
        // setup the field representing CPUs earliest free slot times
        predictFirstFreeSlots(current_time);
    }

    /**
     * Queue only method (not to be used with schedules) - gets first available start time for gridlet corresponding to gi parameter
     */
    public double getEarliestStartTime(GridletInfo gi, double current_time) {
        // updates finishTimeOnPE
        this.updateFinishTimeOfAssignedGridlets(current_time);
        // get EST according to gi PE count
        int index = findFirstFreeSlot(finishTimeOnPE, gi);
        this.est = finishTimeOnPE[index]; // Earl. Start Time for head of queue
        this.usablePEs = findUsablePEs(index, finishTimeOnPE, gi);

//        for(int i = 0; i<)

        return this.est;
    }

    /**
     * This method tries to find a suitable hole (gap) for gridlet gi in current schedule. Suitable == long enough, large enough wrt. PEs.
     */
    public boolean findHoleForGridlet(GridletInfo gi) {
        if (gi.getNumPE() > this.numPE) {
            return false;
        }
        double mips = gi.getJobRuntime(peRating) * peRating;
        Hole candidate = null;
        double prev_end = Double.MAX_VALUE;

        for (int i = 0; i < holes.size(); i++) {
            Hole h = (Hole) holes.get(i);

            if (h.getSize() >= gi.getNumPE() && h.getStart() <= prev_end) {
                if (candidate == null) {
                    // new candidate hole
                    candidate = h;
                }
                // next hole has to start right after this hole
                prev_end = h.getEnd();

                // hole(s) are large enough
                if (mips <= h.getMips()) {
                    // what is the candidate position in schedule
                    GridletInfo nextGi = (GridletInfo) candidate.getPosition(); // because of this Gi the hole(s) were created

                    int index = 0;
                    if (nextGi == null) {
                        index = resSchedule.size();
                    } else {
                        index = resSchedule.indexOf(nextGi);
                    }

                    this.addGInfo(index, gi);
                    return true;
                } else {
                    // hole(s) are still small
                    // decrease remaining length of hole
                    mips = mips - h.getMips();
                }
            } else {
                // restart search for hole - this one is not good (small PEs size)
                candidate = null;
                mips = gi.getJobRuntime(peRating) * peRating;
                prev_end = Double.MAX_VALUE;
                // this gap is candidate in the next round (otherwise it would be skipped, resulting in a possible error)
                if (h.getSize() >= gi.getNumPE()) {
                    i--;
                }
            }
        }
        System.out.println("No hole found for gi=" + gi.getID() + " which is weird because holes=" + holes.size());
        return false;
    }

    /**
     * Method that print all holes in current schedule.
     */
    protected void printHole() {
        if (holes.size() > 0) {
            Hole h = (Hole) holes.getFirst();
            GridletInfo gi = (GridletInfo) resSchedule.get(0);
            System.out.println(holes.size() + "\t" + resSchedule.size() + " | " + h.getPosition().getID() +
                    "," + gi.getID() + " | " + h.getLength());
        }
    }

    /**
     * Method that print current schedule.
     */
    protected void printSchedule() {
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gi = (GridletInfo) resSchedule.get(j);
            System.out.print(gi.getNumPE() + ",");
        }
        System.out.println(" on RES " + this.resource.getResourceID());
    }

    /*
     * Auxiliary function to realize whether this resource supports job's requirements
     *
     */
    protected boolean supportProperty(String prop) {
        String supported = this.resource.getProperties();
        //System.out.println("SUPPROTED by RI = "+supported);
        if (supported.contains(prop)) {
            return true;
        } else {
            return false;
        }
    }

    /** Finds the position of specified gridlet in the resource's schedule. */
    public int findGridletInfoPosition(GridletInfo gi) {
        for (int j = 0; j < resSchedule.size(); j++) {
            GridletInfo gs = (GridletInfo) resSchedule.get(j);
            if (gs.getID() == gi.getID() && gs.getUser().equals(gi.getUser())) {
                return j;
            }
        }
        return -1;
    }

    /** Updates Fairness related criteria. */
    public double[] updateFairness(double[] tuwt, double[] tusa) {
        // update when internal inf. is stable and the int. array is not, or when the size of internal array is different from Scheduler's array size, or the int. inf. is not stable
        if (r_tuwt == null | (stable && !stable_w) || (r_tuwt.length != (tuwt.length * 2)) || !stable) {
            int size = tuwt.length;

            // initialize new array
            r_tuwt = new double[size * 2];
            for (int i = 0; i < r_tuwt.length; i++) {
                r_tuwt[i] = 0.0;
            }


            for (int i = 0; i < resInExec.size(); i++) {
                GridletInfo gi = resInExec.get(i);
                int user_index = Scheduler.users.indexOf(new String(gi.getUser()));
                r_tuwt[user_index] += Math.max(0.0, gi.getGridlet().getExecStartTime() - gi.getRelease_date());
                r_tuwt[user_index + size] += gi.getNumPE() * gi.getJobRuntime(peRating);
            }
            for (int i = 0; i < resSchedule.size(); i++) {
                GridletInfo gi = resSchedule.get(i);
                int user_index = Scheduler.users.indexOf(new String(gi.getUser()));
                r_tuwt[user_index] += Math.max(0.0, gi.getExpectedStartTime() - gi.getRelease_date());
                r_tuwt[user_index + size] += gi.getNumPE() * gi.getJobRuntime(peRating);
            }
            stable_w = true;
        }

        return r_tuwt;
    }
}
