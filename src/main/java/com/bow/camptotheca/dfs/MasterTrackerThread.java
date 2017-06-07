package com.bow.camptotheca.dfs;

import com.bow.camptotheca.dfs.ElectionService.MasterTracker;

/**
 * Thread Class for Tracking Master as SDFSProxy
 *
 */
public class MasterTrackerThread extends Thread {
    private MasterTracker MT;

    public MasterTrackerThread(MasterTracker MT) {
        this.MT = MT;
    }

    @Override
    public void run() {
        MT.startMT();
    }
}
