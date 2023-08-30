package io.ytong.chs;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.ytong.chs.entity.Server;
import io.ytong.chs.entity.Task;
import io.ytong.chs.entity.TaskType;
import io.ytong.chs.helper.HashUtilHelper;
import io.ytong.chs.helper.PropertyHelper;

public class Scheduler {
	private static final int VIRTUAL_NODE_NUM = 10;
	private static final String GENERAL_RING_NAME= "General";
	
	private PropertyHelper propertyHelper;
	private final double IMBALANCE_FACTOR;
	private final double BOUND_LOAD_THRESHOLD_FACTOR;
	
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	
	private int loadSum;
	private int serverSum;
	private double maxAssignedLoad;
	private int bound_load_threshold;
	private Map<String, Map<String, Server>> servers;
	private Map<String, SortedMap<Integer, String>> hashRings = new ConcurrentHashMap<String, SortedMap<Integer,String>>();

	private String getVirtualNodeName(String ip, int num) {
		return ip + "&&VN" + String.valueOf(num);
	}
	
	private String getServerName(String virtualNode) {
		return virtualNode.split("&&")[0];
	}
	
	synchronized private void updateMaxAssignedLoad(int weight) {
		this.loadSum += weight;
		this.maxAssignedLoad = (this.loadSum / this.serverSum) * IMBALANCE_FACTOR;
		this.bound_load_threshold = (int) (this.serverSum / TaskType.values().length * BOUND_LOAD_THRESHOLD_FACTOR);
	}
	
	private void updateHashRings() {
		// clean up all hash rings
		this.hashRings.clear();
		
		// update general and task specific hash ring
		for (String keyName: this.servers.keySet()) {
			SortedMap<Integer, String> hashRing = new ConcurrentSkipListMap<Integer, String>();
			for (Server server : this.servers.get(keyName).values()) {
				for (int i=0; i<VIRTUAL_NODE_NUM; i++) {
					String virtualNodeName = getVirtualNodeName(server.getIp(), i);
					int hashVal = HashUtilHelper.getHashVal(virtualNodeName);
					//System.out.println("[" + virtualNodeName + "] launched @ " + hashVal);
					hashRing.put(hashVal, virtualNodeName);
				}
			}
			this.hashRings.put(keyName, hashRing);
		}
	}
	
	public Scheduler(List<Server> servers, PropertyHelper propertyHelper) {
		// set properties
		this.propertyHelper = propertyHelper;
		this.IMBALANCE_FACTOR = this.propertyHelper.getImbalanceFacotr();
		this.BOUND_LOAD_THRESHOLD_FACTOR = this.propertyHelper.getBoundLoadThresholdFactor();
		
		// define servers and hash rings
		this.servers = new ConcurrentHashMap<String, Map<String, Server>>();
		this.hashRings = new ConcurrentHashMap<String, SortedMap<Integer, String>>();
				
		// initialize general servers and hash ring
		this.loadSum = 0;
		this.serverSum = 0;
		Map<String, Server> generalServers = new ConcurrentHashMap<String, Server>();
		SortedMap<Integer, String> generalRing = new ConcurrentSkipListMap<Integer, String>();
		for (Server server: servers) {
			this.serverSum++;
			generalServers.put(server.getIp(), server);
			for (int i=0; i<VIRTUAL_NODE_NUM; i++) {
				String virtualNodeName = getVirtualNodeName(server.getIp(), i);
				int hashVal = HashUtilHelper.getHashVal(virtualNodeName);
				//System.out.println("[" + virtualNodeName + "] launched @ " + hashVal);
				generalRing.put(hashVal, virtualNodeName);
			}
		}
		this.servers.put(GENERAL_RING_NAME, generalServers);
		this.hashRings.put(GENERAL_RING_NAME, generalRing);
		
		// initialize task specific servers and hash rings
		for (TaskType type : TaskType.values()) {
			this.servers.put(type.toString(), new ConcurrentHashMap<String, Server>());
			this.hashRings.put(type.toString(), new ConcurrentSkipListMap<Integer, String>());
        }
	}
	
	public Server scheduleTask(Task task) {
		// update max assigned load	
		updateMaxAssignedLoad(task.getWeight());
				
		// search task specific ring, then search general ring
		int hashVal = HashUtilHelper.getHashVal(task.getId());
		SortedMap<Integer, String> taskRing = this.hashRings.get(task.getType().toString());
		Server server = null;
		String serverIp = "";
		String virtualNode = "";
		int vnHashKey;
		
		boolean skipTaskRing = true;
		
		try {
			//rwl.readLock().lock();
			if (!taskRing.isEmpty()) {	
				SortedMap<Integer, String> subRing = taskRing.tailMap(hashVal);
				if (subRing == null || subRing.isEmpty()) {
					vnHashKey = taskRing.firstKey();
				} else {
					vnHashKey = subRing.firstKey();
				}
				virtualNode = taskRing.get(vnHashKey);
				serverIp = getServerName(virtualNode);
				server = this.servers.get(task.getType().toString()).get(serverIp);
				
				// when selected server with load larger than max assigned load, need find another server with less load
				if (server.getLoad() > this.maxAssignedLoad) {	
					Map<String, Server> taskServers = this.servers.get(task.getType().toString());
					// specific task servers size is larger than bound load threshold, assign task with existing specific task servers
					// don't assign to new server in the general ring, don't increase capacity for a specific type of task
					if (taskServers.size() > bound_load_threshold) {
						// has server with load less than max assigned load, do task ring
						for (Server taskServer : taskServers.values()) {
							if (taskServer.getLoad() < this.maxAssignedLoad) {
								skipTaskRing = false;
								break;
							}
						}					
						if (!skipTaskRing) {
							do {
								// find next virtual node in the ring
								subRing = taskRing.tailMap(vnHashKey + 1);
								if (subRing == null || subRing.isEmpty()) {
									vnHashKey = taskRing.firstKey();	
								} else {
									vnHashKey = subRing.firstKey();
								}
								virtualNode = taskRing.get(vnHashKey);
								serverIp = getServerName(virtualNode);
								server = this.servers.get(task.getType().toString()).get(serverIp);
							} while (server.getLoad() > this.maxAssignedLoad);
						}
					}
				} else {
					// find server, will not use general ring to select server
					skipTaskRing = false;
				}
			} 
		} finally {
			//rwl.readLock().unlock();
		}
		
		// when task ring is empty or decide to skip task ring, use general ring to select a server
		if (taskRing.isEmpty() || skipTaskRing) {
			try {
				//rwl.readLock().lock();
				SortedMap<Integer, String> generalRing = this.hashRings.get(GENERAL_RING_NAME);
				SortedMap<Integer, String> subRing = generalRing.tailMap(hashVal);
				if (subRing == null || subRing.isEmpty()) {
					vnHashKey = generalRing.firstKey();
				} else {
					vnHashKey = subRing.firstKey();
				}
				virtualNode = generalRing.get(vnHashKey);
				serverIp = getServerName(virtualNode);
				server = this.servers.get(GENERAL_RING_NAME).get(serverIp);
				
				// using bounded load consistent hashing algorithm
				if (server.getLoad() > this.maxAssignedLoad) {
					do {
						// find next virtual node in the ring
						subRing = generalRing.tailMap(vnHashKey + 1);
						if (subRing == null || subRing.isEmpty()) {
							vnHashKey = generalRing.firstKey();	
						} else {
							vnHashKey = subRing.firstKey();
						}
						virtualNode = generalRing.get(vnHashKey);
						serverIp = getServerName(virtualNode);
						server = this.servers.get(GENERAL_RING_NAME).get(serverIp);
					} while (server.getLoad() > this.maxAssignedLoad);
				}
			} finally {
				//rwl.readLock().unlock();
			}
			
			// update task specific ring by schedule result
			try {
				//rwl.writeLock().lock();
				this.servers.get(task.getType().toString()).put(server.getIp(), server);
				for (int i=0; i<VIRTUAL_NODE_NUM; i++) {
					String virtualNodeName = getVirtualNodeName(serverIp, i);
					hashVal = HashUtilHelper.getHashVal(virtualNodeName);
					//System.out.println("[" + virtualNodeName + "] launched @ " + hashVal);
					taskRing.put(hashVal, virtualNodeName);	
				}
			} finally {
				//rwl.writeLock().unlock();
			}
		} 
		
		// add this task the server
		// System.out.printf("[SERVER] %s schedule [TASK] %s with type %s \n", server.getIp(), task.getId(), task.getType().toString());
		server.addTask(task);
		return server;
	}
	
	public void addServer(Server server) {
		// update general and task specific servers
		this.serverSum++;
		this.servers.get(GENERAL_RING_NAME).put(server.getIp(), server);
		updateHashRings();
	}
	
	public void removeServer(Server server) {
		// update general and task specific servers
		this.serverSum--;
		for (Map<String, Server> serverMap : this.servers.values()) {
			serverMap.remove(server.getIp());
		}
		updateHashRings();
	}
}
