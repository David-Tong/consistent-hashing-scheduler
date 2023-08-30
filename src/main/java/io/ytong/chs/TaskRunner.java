package io.ytong.chs;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.ytong.chs.entity.Task;
import io.ytong.chs.helper.TaskHelper;

public class TaskRunner implements Runnable {	
	private int taskNum;
	private int sleepDuration;
	private int runTimes;
	private Scheduler scheduler;
	private TaskHelper taskHelper;
	
	public TaskRunner(int taskNum, int sleepDuration, int runTimes, Scheduler scheduler, TaskHelper taskHelper) {
		this.taskNum = taskNum;
		this.sleepDuration = sleepDuration;
		this.runTimes = runTimes;
		this.scheduler = scheduler;
		this.taskHelper = taskHelper;
	}
	
	public void run() {
		try {
			List<Task> tasks = this.taskHelper.getTasks(this.taskNum);
			int count = 0;
	    	long startTime = System.currentTimeMillis();
	    	long endTime = System.currentTimeMillis();
	    	for (int i = 0; i < runTimes; i++) {
		    	for (Task task : tasks) {
		    		count++;
		    		if (count % 100 == 0) {
		    			endTime = System.currentTimeMillis();
		    			System.out.printf("[THREAD] %s [RUN TIME] %s for [COUNT] %s \n", 
		    					Thread.currentThread().getName(), (endTime - startTime) + "ms", count);
		    			startTime = endTime;
		    		}
		    		this.scheduler.scheduleTask(task);
		    	}
		    	if (i < runTimes) {
		    		System.out.printf("[THREAD] %s sleep for next batch. \n", 
	    					Thread.currentThread().getName(), (endTime - startTime) + "ms", count);
		    		TimeUnit.SECONDS.sleep(sleepDuration);
		    	}
	    	}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}

	public int getTaskNum() {
		return taskNum;
	}

	public void setTaskNum(int taskNum) {
		this.taskNum = taskNum;
	}

	public int getSleepDuration() {
		return sleepDuration;
	}

	public void setSleepDuration(int sleepDuration) {
		this.sleepDuration = sleepDuration;
	}

	public Scheduler getScheduler() {
		return scheduler;
	}

	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	public TaskHelper getTaskHelper() {
		return taskHelper;
	}

	public void setTaskHelper(TaskHelper taskHelper) {
		this.taskHelper = taskHelper;
	}
	
}
