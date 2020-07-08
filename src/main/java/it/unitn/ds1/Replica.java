package it.unitn.ds1;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

// The Replica actor
public class Replica extends AbstractActor {

  public static class GenericMessage implements Serializable{}

  public static class CrashMsg extends GenericMessage{}

  //heartbeat msg sent by the coordinator
  public static class CoordinatorHeartBeat extends GenericMessage{}
  //coordinator notify itself that it is time to spawn a heartbeat msgs
  public static class SendHeartBeat extends GenericMessage{}
  //replicas notify themselves about the coordinator heartbeat not arrived
  public static class HeartBeatNotArrived extends GenericMessage{}

  private static int INTERVAL_HEARTBEAT = 1500;
  private static int TIMEOUT_HEARTBEAT = 4500;

  public static class JoinGroupMsg extends GenericMessage {
    public final List<ActorRef> group; // list of group members
    public final ActorRef coordinatorRef; // coordinator reference
    public JoinGroupMsg(List<ActorRef> group, ActorRef coordinatorRef) {
      this.group = Collections.unmodifiableList(group);
      this.coordinatorRef = coordinatorRef;
    }
  }

  public static enum RequestType {READ, WRITE};
  // This class represents a message request (r/w) from a client
  public static class Request extends GenericMessage{
    public final ActorRef client;
    public final RequestType rtype;
    public final Integer v;

    /*
      The following is a method to make the building of a read request easier
    */
    public Request(ActorRef client, RequestType rtype){
      this.client = client;
      this.rtype = rtype;//this will be a read request (write without value has no sense)
      this.v = null;//value does not need to be set in case of read request
    }

    /*
      Client request for read or write request which will be send to a replica
    */
    public Request(ActorRef client, RequestType rtype, Integer v) {
      this.client = client;
      this.rtype = rtype;
      if(rtype == RequestType.READ)//value does not need to be set in case of read request
        this.v = null;
      else
        this.v = v;
    }
  }

  // This class represents a message response (r) from a client
  public static class Response extends GenericMessage{
    public final Integer v;
    public Response(Integer v) {
      this.v = v;
    }
  }

  // Update messages to be sent in broadcast to all replicas by the coordinator
  public static class Update extends GenericMessage{
    public final LocalTime clock;
    public final Integer v;
    public Update(LocalTime clock, Integer v){
      this.clock = clock;
      this.v = v;
    }
    private static Update toUpdate(Object obj){//if an object has Update-like fields, it will be cloned in an Update obj
      if(obj instanceof UpdateACK)
        return new Update(((UpdateACK)obj).clock, ((UpdateACK)obj).v);
      if(obj instanceof WriteOK)
        return new Update(((WriteOK)obj).clock, ((WriteOK)obj).v);
      return null;
    }
  }

  // Update ack messages to be sent to coordinator
  public static class UpdateACK extends GenericMessage{
    public final LocalTime clock;
    public final Integer v;
    public UpdateACK(LocalTime clock, Integer v){
      this.clock = clock;
      this.v = v;
    }
  }

  // Update ack messages to be sent to coordinator
  public static class WriteOK extends GenericMessage{
    public final LocalTime clock;
    public final Integer v;
    public WriteOK(LocalTime clock, Integer v){
      this.clock = clock;
      this.v = v;
    }
  }

  private Random rnd = new Random();
  // replicas that hold value v to be read and/or modified
  private List<ActorRef> replicas;
  // value v hold by every replica
  private int v;
  // coordinator reference
  private ActorRef coordinatorRef;
  // timer for handling coordinator heartbeat (e.g. keep alive msg)
  Cancellable timerHeartBeat;
  // is the replica a coordinator
  private boolean coordinator;
  //hashmap containing number of ack received for a given update 
  private HashMap<LocalTime, Integer> ackMap = new HashMap<> ();
  //stack for pending updates
  private ArrayDeque<Update> pendingUpdates = new ArrayDeque<>();
  //local time wrt this replica, which consists in epoch+sequence number
  private LocalTime clock;
  //has the replica crashed?
  private boolean crashed;

  public Replica(int v, boolean coordinator){
    this.v = v;
    this.coordinator = coordinator;
    clock = new LocalTime(0,0);
    crashed = false;
  }

  // Some stuff required by Akka to create actors of this type
  static public Props props(int v, boolean coordinator) {
    return Props.create(Replica.class, () -> new Replica(v, coordinator));
  }

  @Override
  public void preStart() {
    System.out.println("Starting heartbeat process");
    if(coordinator)
      timerHeartBeat = getContext().system().scheduler().scheduleWithFixedDelay(
        Duration.create(1, TimeUnit.MILLISECONDS),               // when to start generating messages
        Duration.create(INTERVAL_HEARTBEAT, TimeUnit.MILLISECONDS),               // how frequently generate them
        getSelf(),                                           // destination actor reference
        new SendHeartBeat(),                                // the message to send
        getContext().system().dispatcher(),                 // system dispatcher
        getSelf()                                           // source of the message (myself)
      );

    else
      timerHeartBeat = getContext().system().scheduler().scheduleWithFixedDelay(
        Duration.create(TIMEOUT_HEARTBEAT, TimeUnit.MILLISECONDS),               // when to start generating messages
        Duration.create(TIMEOUT_HEARTBEAT, TimeUnit.MILLISECONDS),               // how frequently generate them
        getSelf(),                                           // destination actor reference
        new HeartBeatNotArrived(),                          // the message to send
        getContext().system().dispatcher(),                 // system dispatcher
        getSelf()                                           // source of the message (myself)
      );

  }

  private void sendMessage(ActorRef receiver, GenericMessage msg){
    if(crashed)//don't answer to msg if state is crashed
      return;

    if(msg.getClass() ==  Update.class)
      System.out.println(getSelf().path().name() + " sending UPDATE msg " + ((Update)msg).v + " " +
              ((Update)msg).clock.epoch + "-" + ((Update)msg).clock.sn + " to " + receiver.path().name());
    if(msg.getClass() ==  UpdateACK.class)
      System.out.println(getSelf().path().name() + " sending UPDATEACK msg " + ((UpdateACK)msg).v + " " +
              ((UpdateACK)msg).clock.epoch + "-" + ((UpdateACK)msg).clock.sn + " to " + receiver.path().name());
    if(msg.getClass() == WriteOK.class)
      System.out.println(getSelf().path().name() + " sending WRITEOK msg " + ((WriteOK)msg).v + " " +
              ((WriteOK)msg).clock.epoch + "-" + ((WriteOK)msg).clock.sn + " to " + receiver.path().name());

    //try{Thread.sleep(2000);}catch(Exception e){}

    receiver.tell(msg, getSelf());

    // simulate network delays using sleep
    try { Thread.sleep(rnd.nextInt(10)); }
    catch (InterruptedException e) { e.printStackTrace(); }

  }
  /*  If broadcast equals true send also to itself
  * */
  private void multicastMessageToReplicas(GenericMessage msg, boolean broadcast){
    if(crashed)//don't answer to msg if state is crashed
      return;

    if(msg.getClass() == Update.class)
      System.out.println(getSelf().path().name() + " multicasting UPDATE msg " + ((Update)msg).v + " " +
              ((Update)msg).clock.epoch + "-" + ((Update)msg).clock.sn);
    if(msg.getClass() == WriteOK.class)
      System.out.println(getSelf().path().name() + " multicasting WRITEOK msg " + ((WriteOK)msg).v + " " +
              ((WriteOK)msg).clock.epoch + "-" + ((WriteOK)msg).clock.sn);

    // randomly arrange replicas
    List<ActorRef> shuffledGroup = new ArrayList<>(replicas);
    Collections.shuffle(shuffledGroup);

    // multicast to all peers in the group
    for (ActorRef r: shuffledGroup)
      if(broadcast || !r.equals(getSelf()))//avoid sending to yourself in multicast
        sendMessage(r, msg);
    
  }

  /*  Check if wokupd can be delivered, i.e. if there isn't any pending updates
  *   such that their clock value is less
  * */
  private boolean canDeliver(Update upd) {
    for(Update pupd : pendingUpdates)
      if(pupd.clock.epoch < upd.clock.epoch || pupd.clock.sn < upd.clock.sn)
        return false;

    return true;
  }

  private void deliver(Update upd){
    //perforn update
    this.v = upd.v;
    System.out.println("Replica " + getSelf().path().name() +
            " update " + upd.clock.epoch + ":" + upd.clock.sn + " " + upd.v);
    //delete from pending updates list
    pendingUpdates.remove(upd);
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    this.replicas = msg.group;
    this.coordinatorRef = msg.coordinatorRef;
  }

  /*  Need to enter in crashed mode
  * */
  private void onCrashMsg(CrashMsg cmsg) {
    crashed = true;
  }

  // Here we define our reaction on the received Request messages
  private void onRequest(Request r) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    ActorRef sender = r.client;
    if(r.rtype == RequestType.READ) {
      System.out.println( "<"+ getSelf().path().name()+ "> received read request from client " + getSender().path().name());
      sendMessage(sender, new Response(v));//return immediately local value

    }else if(r.rtype == RequestType.WRITE && !coordinator) {//forward write request to coordinator
      System.out.println( "<"+ getSelf().path().name()+ "> received write request from client " + getSender().path().name());
      sendMessage(coordinatorRef, r);

    }else if(r.rtype == RequestType.WRITE && coordinator){//I'm the coordinator, tell other replicas about the update
      System.out.println( "<"+ getSelf().path().name()+ "> received write request from client " + getSender().path().name());
      clock = LocalTime.newSequence(clock);//update clock
      multicastMessageToReplicas(new Update(clock, r.v), true);
    }

  }

  // Here we define our reaction on the received Update messages
  private void onUpdate(Update upd) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    pendingUpdates.addLast(upd);//add to pending updates to be performed
    for(Update pupd:pendingUpdates)
      System.out.println("<" + getSelf().path().name() + "> pending update: " + pupd.v + " " + pupd.clock.epoch + "-" + pupd.clock.sn);

    sendMessage(coordinatorRef, new UpdateACK(upd.clock, upd.v));
  }

  // Here we define our reaction on the received UpdateACK messages
  private void onUpdateACK(UpdateACK upd) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    //Update hashmap of clock, acks
    Integer acksReceived = ackMap.get(upd.clock);
    if(acksReceived == null){
      acksReceived = 1;
      ackMap.put(upd.clock, 1);
    }else{
      acksReceived++;
      ackMap.put(upd.clock, acksReceived);
    }

    int quorum = (((int) replicas.size())/2) + 1;
    if(acksReceived > quorum){//WRITEOK
      multicastMessageToReplicas(new WriteOK(upd.clock, upd.v), true);
    }

  }

  // Here we define our reaction on the received WriteOK messages, i.e. perform update
  private void onWriteOK(WriteOK wokUpd) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    if(canDeliver(Update.toUpdate(wokUpd))){//check if wokupd can be performed
      //find update within pending updates
      Update updDel = null;
      for (Update update : pendingUpdates) {
        if(update.clock.equals(wokUpd.clock)){
          updDel = update;
        }
      }

      if(updDel != null){
        deliver(updDel);
        //check if other pending updates can be delivered
        for(Update upd : pendingUpdates){
          if(canDeliver(upd))
            deliver(upd);
        }
      }
    }

  }

  /*  Coordinator heartbeat to signal the fact that it is still up
  * */
  private void onCoordinatorHeartBeat(CoordinatorHeartBeat msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;
    System.out.println("" + getSelf().path().name() + " received heartbeat from coordinator");

    if(!coordinator){
      timerHeartBeat.cancel();
      timerHeartBeat = getContext().system().scheduler().scheduleWithFixedDelay(
              Duration.create(TIMEOUT_HEARTBEAT, TimeUnit.MILLISECONDS),               // when to start generating messages
              Duration.create(TIMEOUT_HEARTBEAT, TimeUnit.MILLISECONDS),               // how frequently generate them
              getSelf(),                                           // destination actor reference
              new HeartBeatNotArrived(),                          // the message to send
              getContext().system().dispatcher(),                 // system dispatcher
              getSelf()                                           // source of the message (myself)
      );
    }

  }

  /*  Coordinator heartbeat to signal the fact that it is still up
   * */
  private void onHeartBeatNotArrived(HeartBeatNotArrived msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    System.out.println("FAILURE OF THE COORDINATOR: heartbeat not arrived");
    //TODO start election process
  }

  /*  Coordinator heartbeat to signal the fact that it is still up
   * */
  private void onSendHeartBeat(SendHeartBeat msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;
    System.out.println("" + getSelf().path().name() + ", aka coordinator sending heartbeats to everyone");

    multicastMessageToReplicas(new CoordinatorHeartBeat(), false);
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(JoinGroupMsg.class, this::onJoinGroupMsg)
      .match(CrashMsg.class, this::onCrashMsg)
      .match(CoordinatorHeartBeat.class, this::onCoordinatorHeartBeat)
      .match(HeartBeatNotArrived.class, this::onHeartBeatNotArrived)
      .match(SendHeartBeat.class, this::onSendHeartBeat)
      .match(Request.class, this::onRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateACK.class, this::onUpdateACK)
      .match(WriteOK.class, this::onWriteOK).build();
  }
}
