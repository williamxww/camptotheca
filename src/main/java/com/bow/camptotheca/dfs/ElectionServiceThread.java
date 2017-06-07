package com.bow.camptotheca.dfs;
import com.bow.camptotheca.dfs.ElectionService.ElectionService;

import java.io.IOException;
/**
 * Thread Class for launching Election Service
 *
 */
public class ElectionServiceThread extends Thread{
	private ElectionService ES;
	public ElectionServiceThread(ElectionService ES){
		this.ES=ES;
	}
	@Override
	public void run(){
		try {
			ES.startES();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
