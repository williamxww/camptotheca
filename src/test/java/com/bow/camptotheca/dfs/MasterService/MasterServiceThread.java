package com.bow.camptotheca.dfs.MasterService;
/**Thread Class for launching Master Service
 *
 */
public class MasterServiceThread extends Thread{
	private MasterService MS;
	public MasterServiceThread(MasterService MS){
		this.MS=MS;
	}
	@Override
	public void run(){
		MS.startMS();
	}
}
