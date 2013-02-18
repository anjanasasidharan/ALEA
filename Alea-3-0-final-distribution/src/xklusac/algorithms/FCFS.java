/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package xklusac.algorithms;

import java.util.Date;
import gridsim.GridSim;
import xklusac.environment.GridletInfo;
import xklusac.environment.ResourceInfo;
import xklusac.environment.Scheduler;

/**
 * Class FCFS<p>
 * Implements FCFS algorithm.
 * @author       Dalibor Klusacek
 */

public class FCFS implements SchedulingPolicy {

    private Scheduler scheduler;

    public FCFS(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void addNewJob(GridletInfo gi) {
        double runtime1 = new Date().getTime();
        Scheduler.queue.addLast(gi);        
        Scheduler.runtime += (new Date().getTime() - runtime1);
        System.out.println("New job has been received by FCFS");
    }

    @Override
    public int selectJob() {
        System.out.println("Selecting job by FCFS...");
        int scheduled = 0;
        ResourceInfo r_cand = null;
        for (int i = 0; i < Scheduler.queue.size(); i++) {
            GridletInfo gi = (GridletInfo) Scheduler.queue.get(i);
            for (int j = 0; j < Scheduler.resourceInfoList.size(); j++) {
                ResourceInfo ri = (ResourceInfo) Scheduler.resourceInfoList.get(j);

                if (Scheduler.isSuitable(ri, gi) && ri.getNumFreeNodes() >= gi.getGridlet().getNumNodes()) {

                    r_cand = ri;
                    break;
                }
            }
            if (r_cand != null) {
                gi = (GridletInfo) Scheduler.queue.remove(i);
                //System.err.println(gi.getID()+" PEs size = "+gi.PEs.size());
                r_cand.addGInfoInExec(gi);
                // set the resource ID for this gridletInfo (this is the final scheduling decision)
                gi.setResourceID(r_cand.resource.getResourceID());
                scheduler.submitJob(gi.getGridlet(), r_cand.resource.getResourceID());
                r_cand.is_ready = true;
                scheduler.sim_schedule(GridSim.getEntityId("Alea_3.0_scheduler"), 0.0, Scheduler.GridletWasSent, gi);
                //System.out.println(GridSim.clock()+": FCFS job submitted");
                scheduled++;
                r_cand = null;
                i--;
            } else {
                return scheduled;
            }
        }

        return scheduled;
    }
}
