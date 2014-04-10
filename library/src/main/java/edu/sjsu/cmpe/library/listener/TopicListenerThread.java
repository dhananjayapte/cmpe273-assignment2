package edu.sjsu.cmpe.library.listener;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
 
public class TopicListenerThread {
	public static Logger log;
 
    public static void startThread(final TopicListener topicListener) {
    	int numThreads = 1;
	    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
 
	    Runnable topicThread = new Runnable() {
 
    	    @Override
    	    public void run() {
    	    	log.info("Starting Topic Listener");
    	    	topicListener.listenToTopic();
    	    }
    	};
 
    	log.info("Submitting the background task");
    	executor.execute(topicThread);
    	log.info("Background task submitted");
    
    	executor.shutdown();
    	log.info("End of background task");
    }
}
