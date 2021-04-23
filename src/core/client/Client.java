package core.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import core.Message;

public class Client {

	private Socket s;

	public Client(String ip, int port) throws UnknownHostException, IOException {
		s = new Socket(ip, port);
	}

	public Message sendReceive(Message msg) {
		try {
			ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(s.getOutputStream()));
			out.writeObject(msg);
			out.flush();

			ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(s.getInputStream()));
			Message response = (Message) in.readObject();

			in.close();
			out.close();
			s.close();
			return response;
		} catch (Exception e) {
			// try {
			// s.close();
			// } catch (Exception _e) {
			// _e.printStackTrace();
			// }
			return null;
		}
	}

}
