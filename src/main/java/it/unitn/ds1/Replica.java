package it.unitn.ds1;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;

// The Replica actor
public class Replica extends AbstractActor {

  public static class GenericMessage implements Serializable{}

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
  // is the replica a coordinator
  private boolean coordinator;
  //hashmap containing number of ack received for a given update 
  private HashMap<LocalTime, Integer> ackMap = new HashMap<> ();
  //stack for pending updates
  private ArrayDeque<Update> pendingUpdates = new ArrayDeque<>();
  //local time wrt this replica, which consists in epoch+sequence number
  private LocalTime clock;

  public Replica(int v, boolean coordinator){
    this.v = v;
    this.coordinator = coordinator;
    clock = new LocalTime(0,0);
  }

  public boolean isCoordinator(){
    return coordinator;
  }

  // Some stuff required by Akka to create actors of this type
  static public Props props(int v, boolean coordinator) {
    return Props.create(Replica.class, () -> new Replica(v, coordinator));
  }

  private void sendMessage(ActorRef receiver, GenericMessage msg){
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

  private void multicastMessageToReplicas(GenericMessage msg){
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

  private boolean canDeliver(WriteOK upd) {
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
    this.replicas = msg.group;
    this.coordinatorRef = msg.coordinatorRef;
  }

  // Here we define our reaction on the received Request messages
  private void onRequest(Request r) {
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
      multicastMessageToReplicas(new Update(clock, r.v));
    }

  }

  // Here we define our reaction on the received Update messages
  private void onUpdate(Update upd) {
    pendingUpdates.addLast(upd);//add to pending updates to be performed
    for(Update pupd:pendingUpdates)
      System.out.println("<" + getSelf().path().name() + "> pending update: " + pupd.v + " " + pupd.clock.epoch + "-" + pupd.clock.sn);

    sendMessage(coordinatorRef, new UpdateACK(upd.clock, upd.v));
  }

  // Here we define our reaction on the received UpdateACK messages
  private void onUpdateACK(UpdateACK upd) {

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
      multicastMessageToReplicas(new WriteOK(upd.clock, upd.v));
    }

  }

  // Here we define our reaction on the received WriteOK messages, i.e. perform update
  private void onWriteOK(WriteOK wokUpd) {
    if(canDeliver(wokUpd)){//check if wokupd can be performed
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

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(JoinGroupMsg.class, this::onJoinGroupMsg)
      .match(Request.class, this::onRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateACK.class, this::onUpdateACK)
      .match(WriteOK.class, this::onWriteOK).build();
  }
}
