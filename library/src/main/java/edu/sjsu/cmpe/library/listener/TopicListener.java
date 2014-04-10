package edu.sjsu.cmpe.library.listener;

import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class TopicListener {
	public static LibraryServiceConfiguration configuration;
	public static BookRepositoryInterface bookRepository;
	private final Logger log = LoggerFactory.getLogger(getClass());

	public void listenToTopic() {
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + configuration.getApolloHost() + ":"
				+ configuration.getApolloPort());
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			connection = factory.createConnection(
					configuration.getApolloUser(),
					configuration.getApolloPassword());
			connection.start();
			Destination dest = new StompJmsDestination(
					configuration.getStompTopicName());
			session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
			consumer = session.createConsumer(dest);
			long waitUntil = 5000;
			String receivedMsg = null;
			while (true) {
				Message msg = consumer.receive(waitUntil);
				if (msg != null) {
					if (msg instanceof TextMessage) {
						receivedMsg = ((TextMessage) msg).getText();
						log.info("receivedMsg::TextMessage ===>" + receivedMsg);
						if ("SHUTDOWN".equals(receivedMsg)) {
							break;
						}
					} else if (msg instanceof StompJmsMessage) {
						StompJmsMessage smsg = ((StompJmsMessage) msg);
						receivedMsg = smsg.getFrame().contentAsString();
						log.info("receivedMsg::StompJmsMessage ===>" + smsg);
						if ("SHUTDOWN".equals(receivedMsg)) {
							break;
						}
					} else {
						log.error("Unexpected message type: " + msg.getClass());
					}

					if (receivedMsg != null) {
						String recMsgArr[] = receivedMsg.split("\"");

						String isbn = recMsgArr[0].split(":")[0];
						String title = recMsgArr[1];
						String category = recMsgArr[3];
						String coverImage = recMsgArr[5];
						Book receivedBook = new Book();

						receivedBook.setIsbn(Long.parseLong(isbn));
						receivedBook.setTitle(title);
						receivedBook.setCategory(category);
						try {
							receivedBook.setCoverimage(new URL(coverImage));
						} catch (MalformedURLException e) {
							e.printStackTrace();
						}
						bookRepository.updateLibrary(receivedBook);
					}
				}// end of if
			}
		} catch (JMSException e) {
			e.printStackTrace();
		} finally {
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
