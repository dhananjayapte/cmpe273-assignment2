package edu.sjsu.cmpe.procurement.jobs;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;

/**
 * This job will run at every 5 second.
 */
@Every("5s")
public class ProcurementSchedulerJob extends Job {
	public static ProcurementServiceConfiguration configuration;
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void doJob() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
			factory.setBrokerURI("tcp://" + configuration.getApolloHost() + ":"
					+ configuration.getApolloPort());
			connection = factory.createConnection(configuration.getApolloUser(),
					configuration.getApolloPassword());

			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination dest = new StompJmsDestination(configuration.getStompQueueName());

			consumer = session.createConsumer(dest);
			log.info("Waiting for messages from /queue/12667.book.orders....");
			long waitUntil = 5; // wait for 5 sec
			while (true) {
				Message msg = consumer.receive(waitUntil);
				if (msg instanceof TextMessage) {
					String body = ((TextMessage) msg).getText();
					log.info("Received message = " + body);

					int isbn = Integer.parseInt(body.split(":")[1]);
					ProcurementService.isbns.add(isbn);

				} else if (msg == null) {
					log.info("No new messages. Exiting due to timeout - "
									+ waitUntil / 1000 + " sec");
					break;
				} else {
					log.error("Unexpected message type: "+ msg.getClass());
				}
			} // end while loop
			
			log.info("Done");
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(consumer!=null){
				try {
					consumer.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
			if(session!=null){
				try {
					session.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
			if(connection!=null){
				try {
					connection.stop();
					connection.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
