package appl;

import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import core.Message;
import core.MessageImpl;
import core.Server;
import core.client.Client;

public class PubSubClient {

	private Server observer;
	private ThreadWrapper clientThread;

	private String clientAddress;
	private int clientPort;

	JSONArray brokers;

	JSONObject currBroker;

	public PubSubClient() {
		// this constructor must be called only when the method
		// startConsole is used
		// otherwise the other constructor must be called
	}

	public PubSubClient(String clientAddress, int clientPort, JSONArray brokers) {
		this.clientAddress = clientAddress;
		this.clientPort = clientPort;
		observer = new Server(clientPort);
		clientThread = new ThreadWrapper(observer);
		clientThread.start();
		this.brokers = brokers;
		this.currBroker = (JSONObject) brokers.get(0);
	}

	public void init() {
		String brokerAddress = (String) this.currBroker.get("ip");
		Long brokerPort = (Long) this.currBroker.get("port");
		this.subscribe(brokerAddress, brokerPort.intValue());
	}

	public void subscribe(String brokerAddress, int brokerPort) {

		Message msgBroker = new MessageImpl();
		msgBroker.setBrokerId(brokerPort);
		msgBroker.setType("sub");
		msgBroker.setContent(clientAddress + ":" + clientPort);
		try {
			Client subscriber = new Client(brokerAddress, brokerPort);
			System.out.println(subscriber.sendReceive(msgBroker).getContent());
		} catch (Exception e) {
			Boolean success = this.connectToBackup();

			if (!success)
				this.stopPubSubClient();
		}
	}

	public void unsubscribe(String brokerAddress, int brokerPort) {

		Message msgBroker = new MessageImpl();
		msgBroker.setBrokerId(brokerPort);
		msgBroker.setType("unsub");
		msgBroker.setContent(clientAddress + ":" + clientPort);
		try {
			Client subscriber = new Client(brokerAddress, brokerPort);
			subscriber.sendReceive(msgBroker);
		} catch (Exception e) {
			Boolean success = this.connectToBackup();

			if (!success)
				this.stopPubSubClient();
		}
	}

	public void publish(String message, String brokerAddress, int brokerPort) {
		Message msgPub = new MessageImpl();
		msgPub.setBrokerId(brokerPort);
		msgPub.setType("pub");
		msgPub.setContent(message);
		try {
			Client publisher = new Client(brokerAddress, brokerPort);
			publisher.sendReceive(msgPub);
		} catch (Exception e) {
			Boolean success = this.connectToBackup();
			brokerAddress = (String) this.currBroker.get("ip");
			brokerPort = ((Long) this.currBroker.get("port")).intValue();
			this.publish(message, brokerAddress, brokerPort);
			if (!success)
				this.stopPubSubClient();
		}
	}

	public List<Message> getLogMessages() {
		return observer.getLogMessages();
	}

	public void stopPubSubClient() {
		System.out.println("Client stopped...");
		observer.stop();
		clientThread.interrupt();
	}

	class ThreadWrapper extends Thread {
		Server s;

		public ThreadWrapper(Server s) {
			this.s = s;
		}

		public void run() {
			s.begin();
		}
	}

	public boolean connectToBackup() {
		Integer nBrokersLeft = this.brokers.size();
		for (int i = 1; i < this.brokers.size(); i++) {
			JSONObject broker = (JSONObject) this.brokers.get(i);

			Boolean lostConnection = (Boolean) broker.get("lostConnection");

			String brokerAddress = (String) broker.get("ip");
			Integer brokerPort = ((Long) broker.get("port")).intValue();

			if (!lostConnection) {
				try {
					System.out.println(
							"Subscribing to " + broker.get("name") + " at " + brokerAddress + ":" + brokerPort);

					this.subscribe(brokerAddress, brokerPort);

					System.out.println("Connected to " + broker.get("name"));

					this.currBroker = broker;
					return true;
				} catch (Exception e) {
					broker.put("lostConnection", true);
					System.out.println("Lost connection to " + broker.get("name") + ", connecting to backup");
				}
			} else {
				nBrokersLeft--;
				this.unsubscribe(brokerAddress, brokerPort.intValue());
			}
		}
		return nBrokersLeft > 0;
	}

}
