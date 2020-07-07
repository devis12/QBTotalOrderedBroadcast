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
import it.unitn.ds1.Message.GenericMessage;
import it.unitn.ds1.Message.Request;
import it.unitn.ds1.Message.RequestType;
import it.unitn.ds1.Message.Response;
import it.unitn.ds1.Message.Update;
import it.unitn.ds1.Message.UpdateACK;
import it.unitn.ds1.Message.JoinGroupMsg;
import it.unitn.ds1.Message.WriteOK;

// The Replica actor
public class Replica extends AbstractActor {
  
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
    receiver.tell(msg, getSelf());

    // simulate network delays using sleep
    try { Thread.sleep(rnd.nextInt(10)); } 
    catch (InterruptedException e) { e.printStackTrace(); }

  }

  private void multicastMessageToReplicas(GenericMessage msg){
    // randomly arrange replicas
    List<ActorRef> shuffledGroup = new ArrayList<>(replicas);
    Collections.shuffle(shuffledGroup);

    // multicast to all peers in the group
    for (ActorRef r: shuffledGroup) 
      sendMessage(r, msg);
    
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    this.replicas = msg.group;
    this.coordinatorRef = msg.coordinatorRef;
  }

  // Here we define our reaction on the received Request messages
  private void onRequest(Request r) {
    ActorRef sender = r.client;
    if(r.rtype == RequestType.READ)
      sendMessage(sender, new Response(v));//return immediately local value
    
    else if(r.rtype == RequestType.WRITE && !coordinator)//forward write request to coordinator
      sendMessage(coordinatorRef, r);

    else if(r.rtype == RequestType.WRITE && coordinator){//I'm the coordinator, tell other replicas about the update
      clock.newSequence();//update clock
      multicastMessageToReplicas(new Update(clock, r.v));
    }

  }

  // Here we define our reaction on the received Update messages
  private void onUpdate(Update upd) {
    pendingUpdates.addFirst(upd);//add to pending updates to be performed
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
      multicastMessageToReplicas(new WriteOK(clock, upd.v));
    }

  }

  // Here we define our reaction on the received WriteOK messages, i.e. perform update
  private void onWriteOK(WriteOK wokUpd) {

    //find update within pending updates
    Update updDel = null;
    for (Update update : pendingUpdates) {
      if(update.clock.equals(wokUpd.clock)){
        updDel = update;
      }
    }

    if(updDel != null){
      //perforn update
      this.v = wokUpd.v;
      System.out.println("Replica " + getSelf().path().name() + 
      " update " + wokUpd.clock.epoch + ":" + wokUpd.clock.sn + " " + wokUpd.v);
      //delete from pending updates list
      pendingUpdates.remove(updDel);
    }
    

  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(JoinGroupMsg.class,    this::onJoinGroupMsg)
      .match(Request.class, this::onRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateACK.class, this::onUpdateACK)
      .match(WriteOK.class, this::onWriteOK).build();
  }
}
