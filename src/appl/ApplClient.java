package appl;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import core.Message;

import java.util.Random;

public class ApplClient {

  public PubSubClient client;
  String name;
  String ip;
  Long port;

  JSONObject currBroker;

  JSONArray brokers;

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    new ApplClient();
  }

  public ApplClient() throws Exception {
    // String fileNameConfig = "clientA.config.json";
    String fileNameConfig = "clientB.config.json";
    // String fileNameConfig = "clientC.config.json";
    File file = new File("src/appl/" + fileNameConfig);

    JSONObject jsonObject = loadJSON(file.getAbsolutePath());

    this.name = (String) jsonObject.get("name");
    this.ip = (String) jsonObject.get("ip");
    this.port = (Long) jsonObject.get("port");

    Long numberOfRequests = (Long) jsonObject.get("numberOfRequests");
    Long maxSleepTime = (Long) jsonObject.get("maxSleepTime");

    this.brokers = (JSONArray) jsonObject.get("brokers");

    System.out.println("Starting client " + this.name + " at " + this.ip + ":" + this.port);

    this.client = new PubSubClient(this.ip, this.port.intValue());

    Boolean isConnected = this.connectToBroker();

    if (!isConnected) {
      System.out.println("Stoping " + this.name);
      this.client.stopPubSubClient();
      return;
    }

    Boolean isRequesting = false;
    Boolean hasAccess = false;

    Integer numberOfTries = 0;
    Integer iterationLimit = 40;

    
    String brokerAddress;
    Long brokerPort;

    do {
      brokerAddress = (String) this.currBroker.get("ip");
      brokerPort = (Long) this.currBroker.get("port");

      try {
        // Check if is alive
        Socket s = new Socket(brokerAddress, brokerPort.intValue());
        s.close();
      } catch (Exception e) {
        // this.currBroker.put("lostConnection", true);
        isConnected = this.connectToBroker();
        System.out.println("isConnected: " + isConnected);
        if (!isConnected)
          break;
      }

      if (!isRequesting) {
        this.makeRequest("Aquire  : var X");
        isRequesting = true;
      }

      hasAccess = checkIfHasAccess();

      if (hasAccess) {
        Random rand = new Random();
        Integer secs = rand.nextInt(maxSleepTime.intValue());

        this.makeRequest("Using   : var X");

        System.out.println(this.name + " is using");

        TimeUnit.SECONDS.sleep(secs); // Simulando a utilização do recurso
        this.makeRequest("Release : var X");

        isRequesting = false;
        numberOfTries++;
      }

      // Espera 2 segundos antes de verificar novamente se tem acesso
      System.out.println(this.name + " is awaiting");
      TimeUnit.SECONDS.sleep(2);
      iterationLimit--;

    } while (iterationLimit > 0 && numberOfTries <= numberOfRequests);

    System.out.println("Stoping " + this.name);

    printLog();

    this.client.unsubscribe(brokerAddress, brokerPort.intValue());
    this.client.stopPubSubClient();
  }

  public void printLog() {
    System.out.println("==================================");
    System.out.println("Printing log:");
    List<Message> log = this.client.getLogMessages();
    Iterator<Message> it = log.iterator();
    while (it.hasNext()) {
      Message aux = it.next();
      System.out.print("- " + aux.getContent() + " | t" + aux.getLogId() + "\n");
    }
    System.out.println();
  }

  class ThreadWrapper extends Thread {
    PubSubClient c;
    String msg;
    String type;
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

  public void makeRequest(String messageContent) {
    String brokerAddress = (String) this.currBroker.get("ip");
    Long brokerPort = (Long) this.currBroker.get("port");
    Thread request = new ThreadWrapper(this.client,
        messageContent + " - " + this.name + " | " + this.currBroker.get("name"), brokerAddress, brokerPort.intValue());
    try {
      request.start();
      request.join();
      request.interrupt();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public boolean connectToBroker() {
    Integer nBrokersLeft = this.brokers.size();
    for (int i = 0; i < this.brokers.size(); i++) {
      JSONObject broker = (JSONObject) this.brokers.get(i);

      Boolean lostConnection = (Boolean) broker.get("lostConnection");

      String brokerAddress = (String) broker.get("ip");
      Long brokerPort = (Long) broker.get("port");

      if (!lostConnection) {
        try {
          // Checa se esta vivo
          Socket s = new Socket(brokerAddress, brokerPort.intValue());
          s.close();

          System.out.println("Subscribing to " + broker.get("name") + " at " + brokerAddress + ":" + brokerPort);
          this.client.subscribe(brokerAddress, brokerPort.intValue());

          System.out.println("Connected to " + broker.get("name"));

          this.currBroker = broker;
          return true;
        } catch (Exception e) {
          broker.put("lostConnection", true);
          System.out.println("Lost connection to " + broker.get("name") + ", connecting to backup");
        }
      } else {
        nBrokersLeft--;
        this.client.unsubscribe(brokerAddress, brokerPort.intValue());
      }
    }
    return nBrokersLeft > 0;
  }

  public static JSONObject loadJSON(String file) throws Exception {
    // Cria um Objeto JSON

    JSONParser parser = new JSONParser();
    JSONObject jsonObject;

    jsonObject = (JSONObject) parser.parse(new FileReader(file));

    return jsonObject;
  }

  public Boolean checkIfHasAccess() {
    List<Message> log = this.client.getLogMessages();
    Iterator<Message> it = log.iterator();

    List<String> openRequests = new ArrayList<String>();

    String currBrokerName = (String) this.currBroker.get("name");

    String clientId = this.name + " | " + currBrokerName;

    while (it.hasNext()) {
      Message message = it.next();
      String request = message.getContent();

      // Se o request é um aquire, add a lista de requests em aberto
      if (request.startsWith("Aquire")) {
        openRequests.add(request);
      }

      // Verifica se o release é do primeiro aquire
      if (request.startsWith("Release")) {

        String[] splitedRequest = request.split("-");
        String requestId = splitedRequest[1].trim(); // "Client A - Broker 1"
        String firstRequest = openRequests.get(0);

        if (firstRequest.endsWith(requestId)) {
          openRequests.remove(firstRequest);
        }
      }
    }

    // Se houver requests em aberto e o primeiro aquire for do cliente, o cliente
    // pode acessar
    if (openRequests.size() > 0) {
      String currentRequest = openRequests.get(0);
      if (currentRequest.endsWith(clientId)) {
        return true;
      }
    }

    return false;
  }
}
