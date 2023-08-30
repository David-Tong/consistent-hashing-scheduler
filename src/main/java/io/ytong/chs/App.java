package io.ytong.chs;

import java.io.IOException;
//import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.ytong.chs.entity.Server;
import io.ytong.chs.helper.PropertyHelper;
import io.ytong.chs.helper.ServerHelper;
import io.ytong.chs.helper.TaskHelper;

public class App {
	
    public static void main( String[] args ) {
    	// load properties
    	PropertyHelper propertyHelper = new PropertyHelper();
    	try {
    		propertyHelper.loadProperties();
    	} catch (IOException ex) {
    		ex.printStackTrace();
    		System.exit(1);
    	} catch (IllegalArgumentException ex) {
    		ex.printStackTrace();
    		System.exit(1);
    	}
    	
    	// setup helpers
    	ServerHelper serverHelper = new ServerHelper(propertyHelper);
    	TaskHelper taskHelper = new TaskHelper();
    	
    	// setup scheduler
    	List<Server> servers = serverHelper.getServers();
    	Scheduler scheduler = new Scheduler(servers, propertyHelper);
    	
    	// start timer
    	long startTime = System.currentTimeMillis();
    	
    	// schedule
    	ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(300);
    	
    	// run task runner at the interval of 5s, 1000 tasks in a batch, 200 runners, run for twice
    	for (int i = 0; i < 200; i++) {
    		TaskRunner taskRunner = new TaskRunner(1000, 1, 5, scheduler, taskHelper);
    		executor.submit(taskRunner);
    	}
    	
    	try {
    		executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}  
    	
    	// stop timer
    	long endTime = System.currentTimeMillis();
    	System.out.printf("[THREAD] %s program takes %s to run \n", 
				Thread.currentThread().getName(), (endTime - startTime) + "ms");
    }
}
