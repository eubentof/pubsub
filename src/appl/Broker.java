package appl;

import core.Server;

import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import core.Message;

public class Broker {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new Broker();
	}

	public Broker() {

		Scanner reader = new Scanner(System.in); // Reading from System.in
		System.out.print("Enter the Broker port number: ");
		int port = reader.nextInt(); // Scans the next token of the input as an int.

		Server s = new Server(port);
		ThreadWrapper brokerThread = new ThreadWrapper(s);
		brokerThread.start();

		String resp;

		do {

			System.out.print("\nType 'stop' and press [enter] to stop broker.\n");
			resp = reader.next();

			if (resp.equals("stop")) {
				System.out.println("Broker stopped.");
				s.stop();
				brokerThread.interrupt();
				System.out.println("==================================");
				System.out.println("Printing log:");
				List<Message> log = s.getLogMessages();
				Iterator<Message> it = log.iterator();
				while (it.hasNext()) {
					Message aux = it.next();
					System.out.print("- " + aux.getContent() + " | t:" + aux.getLogId() + "\n");
				}
				System.out.println();
			}

		} while (!resp.equals("stop"));

		// once finished
		reader.close();
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

}
