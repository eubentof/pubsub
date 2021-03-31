package appl;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Array;
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

  JSONArray brokers;

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    new ApplClient();
  }

  public ApplClient() throws Exception {
    String fileNameConfig = "clientA.config.json";
    // String fileNameConfig = "clientB.config.json";
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

    for (int i = 0; i < this.brokers.size(); i++) {
      JSONObject broker = (JSONObject) this.brokers.get(i);

      String brokerAddress = (String) broker.get("ip");
      Long brokerPort = (Long) broker.get("port");

      System.out.println("Subscribing to " + broker.get("name") + " at " + brokerAddress + ":" + brokerPort);

      this.client.subscribe(brokerAddress, brokerPort.intValue());
    }

    // TODO: TP2 - Colocar para pegar outros brokers
    JSONObject broker = (JSONObject) this.brokers.get(0);
    String brokerAddress = (String) broker.get("ip");
    Long brokerPort = (Long) broker.get("port");

    Boolean isRequesting = false;
    Boolean hasAccess = false;

    Integer numberOfTries = 0;
    Integer iterationLimit = 10;

    Thread request = new ThreadWrapper(this.client, "Aquire - var X | " + this.name, brokerAddress,
        brokerPort.intValue());
    request.start();
    request.join();
    request.interrupt();
    isRequesting = true;
    try {
      while (iterationLimit > 0 && numberOfTries <= numberOfRequests) {

        if (!isRequesting) {
          // Solicitar o acesso
          request = new ThreadWrapper(this.client, "Aquire - var X | " + this.name, brokerAddress,
              brokerPort.intValue());
          request.start();
          request.join();
          request.interrupt();
          isRequesting = true;
        }

        hasAccess = checkIfHasAccess();

        if (hasAccess) {
          Random rand = new Random();
          Integer secs = rand.nextInt(maxSleepTime.intValue());

          request = new ThreadWrapper(this.client, "Using - var X | " + this.name, brokerAddress,
              brokerPort.intValue());
          request.start();
          request.join();
          request.interrupt();

          TimeUnit.SECONDS.sleep(secs); // Simulando a utilização do recurso

          request = new ThreadWrapper(this.client, "Release - var X | " + this.name, brokerAddress,
              brokerPort.intValue());
          request.start();
          request.join();
          request.interrupt();

          isRequesting = false;
          numberOfTries++;
        }
        // Espera 2 segundos antes de verificar novamente se tem acesso
        TimeUnit.SECONDS.sleep(2);
        iterationLimit--;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("Stoping " + this.name);

    // TODO: Colocar aqui com o broker salvo
    this.client.unsubscribe("localhost", 8080);
    this.client.stopPubSubClient();
  }

  public void printLog() {
    System.out.println("==================================");
    System.out.println("Printing log:");
    List<Message> log = this.client.getLogMessages();
    Iterator<Message> it = log.iterator();
    while (it.hasNext()) {
      Message aux = it.next();
      System.out.print("- " + aux.getContent() + " | id" + aux.getLogId() + "\n");
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

    while (it.hasNext()) {
      Message message = it.next();
      String content = message.getContent();

      // Verifica todos os requests
      if (content.startsWith("Aquire")) {
        openRequests.add(content);
      }

      // Verifica todos as releases
      if (content.startsWith("Relase")) {
        for (String request : openRequests) {
          String clientName = request.split("|")[1].strip();

          if (request.endsWith(clientName)) {
            openRequests.remove(request);
            break;
          }
        }
      }

      if (openRequests.size() > 0) {
        String currentRequest = openRequests.get(0);
        if (currentRequest.endsWith(this.name)) {
          return true;
        }
      }
    }
    return false;
  }
}
