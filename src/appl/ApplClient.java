package appl;

import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Iterator;
import java.util.List;

import core.Message;

public class ApplClient {

  public PubSubClient client;
  String name;
  String ip;
  Long port;

  JSONArray brokers;

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    new ApplClient();
  }

  public ApplClient() throws Exception {
    String filePath = "clientA.config.json";
    File file = new File("src/appl/" + filePath);

    JSONObject jsonObject = loadJSON(file.getAbsolutePath());

    this.name = (String) jsonObject.get("name");
    this.ip = (String) jsonObject.get("ip");
    this.port = (Long) jsonObject.get("port");

    this.brokers = (JSONArray) jsonObject.get("brokers");

    System.out.println("Starting client " + name + " at " + ip + ":" + port);

    this.client = new PubSubClient(ip, port.intValue());

    for (int i = 0; i < this.brokers.size(); i++) {
      JSONObject broker = (JSONObject) this.brokers.get(i);

      String brokerAddress = (String) broker.get("ip");
      Long brokerPort = (Long) broker.get("port");

      System.out.println("Subscribing to " + broker.get("name") + " at " + brokerAddress + ":" + brokerPort);

      this.client.subscribe(brokerAddress, brokerPort.intValue());
    }

    for (int i = 0; i < 5; i++) {
      JSONObject broker = (JSONObject) this.brokers.get(0);
      String brokerAddress = (String) broker.get("ip");
      Long brokerPort = (Long) broker.get("port");

      try {
        Thread request = new ThreadWrapper(this.client, "Access " + this.name + " - var X", brokerAddress,
            brokerPort.intValue());
        request.start();
        request.join();
        TimeUnit.SECONDS.sleep(5);
        request.interrupt();
      } catch (Exception e) {
        throw (e);
      }
    }
    
    System.out.println("Printing log:");
    List<Message> log = this.client.getLogMessages();
    Iterator<Message> it = log.iterator();
    while (it.hasNext()) {
      Message aux = it.next();
      System.out.print("- " + aux.getContent() + aux.getLogId() + "\n");
    }
    System.out.println();
    
    System.out.println("Stoping " + this.name + "...");
    // TODO: Colocar aqui com o broker salvo
    this.client.unsubscribe("localhost", 8080);
    this.client.stopPubSubClient();
  }

  class ThreadWrapper extends Thread {
    PubSubClient c;
    String msg;
    String host;
    int port;

    public ThreadWrapper(PubSubClient c, String msg, String host, int port) {
      this.c = c;
      this.msg = msg;
      this.host = host;
      this.port = port;
    }

    public void run() {
      c.publish(msg, host, port);
    }
  }

  public static JSONObject loadJSON(String file) throws Exception {
    // Cria um Objeto JSON

    JSONParser parser = new JSONParser();
    JSONObject jsonObject;

    jsonObject = (JSONObject) parser.parse(new FileReader(file));

    return jsonObject;
  }
}
