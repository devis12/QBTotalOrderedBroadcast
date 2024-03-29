package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import it.unitn.ds1.Replica.Request;
import it.unitn.ds1.Replica.RequestType;
import it.unitn.ds1.Replica.Response;
import scala.concurrent.duration.Duration;

// The Client actor
public class Client extends AbstractActor {

  private static final String logFolderPath = "logs";

  private static int TIMEOUT_READ = 3000;

  //used externally from the system to trigger a client, so it performs a read request
  public static class SendReadRequest implements Serializable {}

  //used externally from the system to trigger a client, so it performs a write request
  public static class SendWriteRequest implements Serializable {
    public final Integer v;
    public SendWriteRequest(Integer v){
      this.v = v;
    }
  }

  public static class TimeoutReadMsg implements Serializable {
    private final Integer idRead;
    public TimeoutReadMsg(Integer idRead){this.idRead = idRead;}
  }

  // replicas that hold value v to be read and/or modified
  private List<ActorRef> replicas;
  // handle timeout for read request
  private HashMap<Integer, Cancellable> timeoutRead = new HashMap<>();
  //read counter
  private int currentIdRead;


  public Client(List<ActorRef> replicas) {
    this.replicas = replicas;
    this.currentIdRead = 0;
  }

  static public Props props(List<ActorRef> replicas) {
    return Props.create(Client.class, () -> new Client(replicas));
  }

  private ActorRef selectRandomReplica(){
    int randomPos = new Random(System.currentTimeMillis()).nextInt(replicas.size());
    return replicas.get(randomPos);
  }

  /*  Append log to logfile
   * */
  private void appendLog(String textToAppend){
    try {
      if(Files.notExists(Paths.get(logFolderPath)))
        Files.createDirectory(Paths.get(logFolderPath));

      Files.write(Paths.get(logFolderPath + File.separator + getSelf().path().name()+"_log.txt"), Arrays.asList(textToAppend), StandardCharsets.UTF_8,
              StandardOpenOption.CREATE, StandardOpenOption.APPEND);  //Append mode
    }catch(IOException ioe){
      System.err.println("[" + getSelf().path().name() + "] IOException while writing in logfile " + Paths.get(getSelf().path().name()+"_log.txt").toAbsolutePath());
    }
  }

  @Override
  public void preStart() {
    //delete previous runs logFile
    try {
      if(Files.exists(Paths.get(logFolderPath + File.separator + getSelf().path().name() + "_log.txt"))) {
        System.out.println("[" + getSelf().path().name() + "] deleting previous runs logfile");
        Files.delete(Paths.get(logFolderPath + File.separator + getSelf().path().name() + "_log.txt"));
      }
    }catch(IOException ioe){
      System.err.println("[" + getSelf().path().name() + "] IOException while writing in logfile " + Paths.get(getSelf().path().name()+"_log.txt").toAbsolutePath());
    }

  }

  // Here we define our reaction on the received message from the replica containing the value we're interested
  private void onResponse(Response r) {
    for(int key: timeoutRead.keySet())
      System.out.println("Client " + getSelf().path().name() + " waiting response for read" + key);
    System.out.println("Client " + getSelf().path().name() + " received response for read" + r.idRead);
    timeoutRead.get(r.idRead).cancel();
    timeoutRead.remove(r.idRead);
    System.out.println("[" +
            getSelf().path().name() +           // the name of the current actor
            "] read done from " +
            getSender().path().name() +         // the name of the sender actor
            ", value: " + r.v                   // finally the message contents
    );

    //log Client <ClientID> read done <value>
    appendLog("Client " + getSelf().path().name() + " read done " + r.v);
  }

  //Method to trigger externally wrt the client to perform a read
  private void onSendReadRequest(SendReadRequest msg) {
    currentIdRead++;
    ActorRef replica = selectRandomReplica(); //read from a random replica
    System.out.println("["+getSelf().path().name()+"] ready to make a read request (idRead = " + currentIdRead + ") to " + replica.path().name());
    //log  Client <ClientID> read req to <ReplicaID>
    appendLog("Client " + getSelf().path().name() + " read req to " + replica.path().name());
    replica.tell(new Request(getSelf(), RequestType.READ, currentIdRead), getSelf());

    //timeout for just performed read
    timeoutRead.put(currentIdRead, getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(TIMEOUT_READ, TimeUnit.MILLISECONDS),               // when to start generating messages
            Duration.create(TIMEOUT_READ, TimeUnit.MILLISECONDS),               // how frequently generate them
            getSelf(),                                              // destination actor reference
            new TimeoutReadMsg(currentIdRead),                                      // the message to send
            getContext().system().dispatcher(),                     // system dispatcher
            getSelf()                                               // source of the message (myself)
    ));
  }

  private void onSendWriteRequest(SendWriteRequest msg) {
    ActorRef replica = selectRandomReplica();
    System.out.println(""+getSelf().path().name()+" ready to make a write request to " + replica.path().name() + " proposing value " + msg.v);
    replica.tell(new Request(getSelf(), RequestType.WRITE, msg.v), getSelf());
  }

  private void onTimeoutRead(TimeoutReadMsg msg) {
    timeoutRead.get(msg.idRead).cancel();//timeout has served its purpose
    timeoutRead.remove(msg.idRead);
    System.out.println("["+getSelf().path().name()+"] read request has FAILED!");
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Response.class, this::onResponse)
      .match(SendReadRequest.class, this::onSendReadRequest)
      .match(SendWriteRequest.class, this::onSendWriteRequest)
      .match(TimeoutReadMsg.class, this::onTimeoutRead)
      .build();
  }

}

