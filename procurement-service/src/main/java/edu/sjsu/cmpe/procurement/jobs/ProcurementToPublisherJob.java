package edu.sjsu.cmpe.procurement.jobs;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;

/**
 * This job will run at every 5 minutes.
 */
@Every("300s")
public class ProcurementToPublisherJob extends Job {
	public static ProcurementServiceConfiguration configuration;
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void doJob() {
		log.info("Executing the 5 minute job");
		
		if (ProcurementService.isbns!= null && !ProcurementService.isbns.isEmpty()) {
			postMessagesToPublisher();
			ProcurementService.isbns = new ArrayList<Integer>();
			getMSgFromPublisher();
		} else {
			log.info("Array is empty");
		}
	}
	/**
	 * This method will place the orders to the Broker
	 */
	public void postMessagesToPublisher() {
		try {
			log.info("Posting msges to publisher!!");

			Client client = Client.create();
			WebResource webResource = client.resource("http://"+configuration.getApolloHost()+":9000/orders");

			String input = "{\"id\":\"12667\",\"order_book_isbns\":" + ProcurementService.isbns + "}";
			log.info("Input JSON creat4d is --->" + input);

			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
			}

			log.info("Output from Server .... \n");
			String output = response.getEntity(String.class);
			log.info(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * This method will retrieve messages from the broker i.e. shipped books and then push the
	 * books to the appropriate topic based on the boook category
	 */
	public void getMSgFromPublisher() {
		String jsonString = ProcurementService.jerseyClient
				.resource("http://"+configuration.getApolloHost()+":9000/orders/12667")
				.type("application/json").get(String.class);
		log.info("Json==>" + jsonString);

		try {
			JSONObject tempJObj = new JSONObject(jsonString);
			JSONArray jArray = tempJObj.getJSONArray("shipped_books");
			for (int i = 0; i < jArray.length(); i++) { // **line 2**
				JSONObject childJSONObject = jArray.getJSONObject(i);
				String categoryName = childJSONObject.getString("category");
				log.info("Category is-->" + categoryName);
				String tempJSON = childJSONObject.getString("isbn") + ":\""
						+ childJSONObject.getString("title") + "\":" + "\""
						+ childJSONObject.getString("category") + "\":" + "\""
						+ childJSONObject.getString("coverimage") + "\"";
				log.info("Newly created JSON as per fomat is---->\n"+ tempJSON);
				if (tempJSON != null) {
					String topicName = configuration.getStompTopicPrefix();
					Destination dest = null;
					if ("computer".equalsIgnoreCase(categoryName)) {
						//push books to computer topic
						topicName = configuration.getStompTopicPrefix() + configuration.getStompTopicComputer();
						dest = new StompJmsDestination(topicName);
						pushBooksToTopics(tempJSON, categoryName, dest);
					}
					//push books to all topic
					topicName = configuration.getStompTopicPrefix() + configuration.getStompTopicAll();
					dest = new StompJmsDestination(topicName);
					pushBooksToTopics(tempJSON, categoryName, dest);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	/**
	 * This method will push the JSON to the given destination topic
	 * @param tempJSON
	 * @param categoryName
	 * @param dest
	 */
	public void pushBooksToTopics(String tempJSON, String categoryName, Destination dest) {
		log.info("Inside pushBooksToTopics");
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + configuration.getApolloHost() + ":"
				+ configuration.getApolloPort());
		Connection connection;
		try {
			connection = factory.createConnection(configuration.getApolloUser(),
					configuration.getApolloPassword());
			connection.start();
			
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(dest);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			TextMessage msg = session.createTextMessage(tempJSON);
			msg.setLongProperty("id", System.currentTimeMillis());
			producer.send(msg);
			log.info("Msg sent to topics");
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

}
