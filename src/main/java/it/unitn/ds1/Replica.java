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

  //interval to generate heartbeat (coordinator)
  private static int INTERVAL_HEARTBEAT = 1500;
  //interval at which replica expects heartbeat
  private static int TIMEOUT_HEARTBEAT = 4500;

  //Timeout when expepecting messages in response from coordinator
  private static int TIMEOUT_MSG = 500;
  //Max time allowed for link communication delays
  private static int MAX_NETWORK_DELAY = 50;

  public static class GenericMessage implements Serializable{}

  public static class CrashMsg extends GenericMessage{}

  //heartbeat msg sent by the coordinator
  public static class CoordinatorHeartBeat extends GenericMessage{}
  //coordinator notify itself that it is time to spawn a heartbeat msgs
  public static class SendHeartBeat extends GenericMessage{}
  //replicas notify themselves about the coordinator heartbeat not arrived
  public static class HeartBeatNotArrived extends GenericMessage{}
  //replicas notify themselves about the coordinator not replying when it is supposed to (i.e. it's crashed)
  public static class CoordinatorTimeout extends GenericMessage{}

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
    /*  Return true if this is more recent than upd
    * */
    public boolean moreRecent(Update upd){
      return  (this.clock.epoch > upd.clock.epoch) //following epoch
              ||
              (this.clock.epoch == upd.clock.epoch && this.clock.sn > upd.clock.sn);//same epoch, following sn
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Update))
        return false;

      Update upd2 = (Update) obj;
      return this.v == upd2.v && this.clock.equals(upd2.clock);
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 53 * hash + this.clock.hashCode();
      hash = 53 * hash + this.v;
      return hash;
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

  // Election message to be sent to next replica up
  public static class ElectionMsg extends GenericMessage{
    public final Update lastUpdate;
    public final ArrayList<ActorRef> lastUpdateHolders;
    public ElectionMsg(Update lastUpdate, ArrayList<ActorRef> lastUpdateHolders){
      this.lastUpdate = lastUpdate;
      this.lastUpdateHolders = lastUpdateHolders;
    }
  }

  // Election message ack to be sent to next replica up
  public static class ElectionMsgACK extends GenericMessage{}

  // Election message to be sent to next replica up
  public static class ElectionACKTimeout extends GenericMessage{
    public final ActorRef receivingReplica;
    public final ElectionMsg electionMsg;
    public ElectionACKTimeout(ActorRef receivingReplica, ElectionMsg electionMsg){
      this.receivingReplica = receivingReplica;
      this.electionMsg = electionMsg;
    }
  }

  // Synchronization message to terminate election
  public static class SynchronizationMsg extends GenericMessage{
    public final ActorRef newCoordinator;
    public final Update updToBePerformed;
    public SynchronizationMsg(ActorRef newCoordinator, Update updToBePerformed){
      this.newCoordinator = newCoordinator;
      this.updToBePerformed = updToBePerformed;
    }
  }


  // replicas that hold value v to be read and/or modified
  private List<ActorRef> replicas;
  // value v hold by every replica
  private int v;
  // coordinator reference
  private ActorRef coordinatorRef;
  // timer for handling coordinator heartbeat (e.g. keep alive msg)
  private Cancellable timerHeartBeat;
  // timer for handling coordinator's failure when replica timeouts waiting for the writeOK
  private HashMap<Update, Cancellable> timersUpdateACK = new HashMap<>();
  // to timeout during election process if not receiving ACK from following replica
  private Cancellable timerElectionMsg;
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
  //election process in progress
  private boolean election;

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
      timerHeartBeat = initTimeout(INTERVAL_HEARTBEAT, INTERVAL_HEARTBEAT, new SendHeartBeat());

    else
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT, TIMEOUT_HEARTBEAT, new HeartBeatNotArrived());
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
    try { Thread.sleep(new Random(System.currentTimeMillis()).nextInt(MAX_NETWORK_DELAY)); }
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

  private Cancellable initTimeout(int startTime, int intervalTime, GenericMessage msg){
    return getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(startTime, TimeUnit.MILLISECONDS),                // when to start generating messages
            Duration.create(intervalTime, TimeUnit.MILLISECONDS),                   // how frequently generate them
            getSelf(),                                                      // destination actor reference
            msg,                                                            // the message to send
            getContext().system().dispatcher(),                             // system dispatcher
            getSelf()                                                       // source of the message (myself)
    );
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

  /*  Discriminate maximum replica by considering their ids, eg "name1", "name2" where name2 is the maximum
  * */
  private boolean isMaxById(ArrayList<ActorRef> lastUpdateHolders, String prefix){
    int currentMax = -1;
    int selfId = Integer.parseInt(getSelf().path().name().substring(prefix.length()));
    for(ActorRef actor : lastUpdateHolders) {
      String name = actor.path().name();
      if (Integer.parseInt(name.substring(prefix.length())) > selfId)
        return false;
    }
    return true;
  }

  /*  Select in the list of replicas the following replica wrt rep
  *   with an option to skip the coordinator ActorRef (useful in case of election)
  * */
  private ActorRef selectNextReplica(ActorRef rep, boolean skipCoordinator){
    int myIndex = -1;//position of the current replica in the ring
    for(int i=0; i<replicas.size();i++)
      if(replicas.get(i) == rep)
        myIndex = i;

    int selectIndex = (myIndex + 1 == replicas.size())? 0 : myIndex + 1; //select next index

    if(skipCoordinator && replicas.get(selectIndex) == coordinatorRef) //in case you want to skip coordinator
      return replicas.get((selectIndex + 1 == replicas.size())? 0 : selectIndex + 1);
    else
      return replicas.get(selectIndex);
  }

  /*  Start election process after coordinator's failure detection
  * */
  private void initiateElection() {
    if(election)
      return;
    System.out.println("["+getSelf().path().name()+"] initiating election");
    election = true;
    //send election message
    ActorRef receivingReplica = selectNextReplica(getSelf(), true);
    ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
    lastUpdateHolders.add(getSelf());
    ElectionMsg newElectionMsg = null;
    if(pendingUpdates.size() > 0)//pick your last update to forward on if there is any
      newElectionMsg = new ElectionMsg(pendingUpdates.getLast(), lastUpdateHolders);
    else
      newElectionMsg = new ElectionMsg(null, lastUpdateHolders);
    sendMessage(receivingReplica, newElectionMsg);

    timerElectionMsg = initTimeout(TIMEOUT_MSG, TIMEOUT_MSG,new ElectionACKTimeout(receivingReplica, newElectionMsg));
  }

  /*  Terminate election process by broadcasting to all, that youp're the new coordinator
  * */
  private void synchronization(){
    System.out.println("["+getSelf().path().name()+"] initiating synchronization to terminate election");
    Update pendingUpdate = (pendingUpdates.size() == 0)? null : pendingUpdates.getLast();
    multicastMessageToReplicas(new SynchronizationMsg(getSelf(), pendingUpdate), true);
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
      if(!election)
        sendMessage(coordinatorRef, r);//forward request to coordinator in normal scenario

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
    //start timer to detect coordinator failure while waiting for writeOK of this update
    timersUpdateACK.put(upd, initTimeout(TIMEOUT_MSG, TIMEOUT_MSG, new CoordinatorTimeout()));
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

    Update confirmedUpdate = Update.toUpdate(wokUpd);

    //cancel timeout handler for this update
    timersUpdateACK.get(confirmedUpdate).cancel();
    timersUpdateACK.remove(confirmedUpdate);

    if(canDeliver(confirmedUpdate)){//check if wokupd can be performed
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
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT, TIMEOUT_HEARTBEAT, new HeartBeatNotArrived());
    }

  }

  /*  replicas notify themselves about the coordinator heartbeat not arrived
   * */
  private void onHeartBeatNotArrived(HeartBeatNotArrived msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;

    System.out.println("FAILURE OF THE COORDINATOR: heartbeat not arrived");
    timerHeartBeat.cancel();
    initiateElection();//start election process
  }

  /*  replicas notify themselves about the coordinator not replying when it is supposed to (i.e. it's crashed)
   * */
  private void onCoordinatorTimeout(CoordinatorTimeout msg) {
    System.out.println("FAILURE OF THE COORDINATOR: reply not arrived");
    initiateElection();//start election process
  }

  /*  Coordinator heartbeat to signal the fact that it is still up
   * */
  private void onSendHeartBeat(SendHeartBeat msg) {
    if(crashed)//don't answer to msg if state is crashed
      return;
    System.out.println("" + getSelf().path().name() + ", aka coordinator sending heartbeats to everyone");

    multicastMessageToReplicas(new CoordinatorHeartBeat(), false);
  }

  private void onElectionMsg(ElectionMsg msg) {
    if (crashed || !election)//don't answer to msg if state is crashed
      return;
    System.out.println("[" + getSelf().path().name() + "] received election msg from " + getSender().path().name());
    election = true;// in case this replica is not already in election mode (e.g. coordinator's timeout detected by someone else)

    sendMessage(getSender(), new ElectionMsgACK());//ack of election msg

    ActorRef receivingReplica = selectNextReplica(getSelf(), true);
    ElectionMsg newElectionmsg;


    if (msg.lastUpdateHolders.contains(getSelf())){

      if(isMaxById(msg.lastUpdateHolders, "replica")){ //election msg has already performed a round and this replica is the best candidate (disambiguate by id, take the maximum)
        System.out.println("["+getSelf().path().name()+"] BEFORE initiating synchronization, best between " + msg.lastUpdateHolders.size() + " candidates");
        synchronization();//broadcast to everyone you're the new coordinator

      }else{//just pass along msg, election has converged, but you're not the best candidate
        newElectionmsg = msg;
        sendMessage(receivingReplica, newElectionmsg);

        timerElectionMsg = initTimeout(TIMEOUT_MSG, TIMEOUT_MSG,new ElectionACKTimeout(receivingReplica, newElectionmsg));
      }


    }else{

      if(pendingUpdates.size() == 0 && msg.lastUpdate == null) {//no pending update is circulating until now and this replica hasn't any
        ArrayList<ActorRef> lastUpdateHolders = msg.lastUpdateHolders;
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(null, lastUpdateHolders);

      }else if(pendingUpdates.size() > 0 && msg.lastUpdate == null) {//mine considered to be the more recent update, since there is none in the election msg
        //NOTE considered this also as the case in which the WRITEOK has just reached some replicas and not all, thus this can be considered as an old update by other replicas
        //TODO handle unfinished update, i.e. case in which WRITEOK has not reached everyone... this can be also considered as a last update to repeat to everyone and things would still be okay (I think...)
        ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(pendingUpdates.getLast(), lastUpdateHolders);

      }else if(pendingUpdates.size() > 0 && (msg.lastUpdate.moreRecent(pendingUpdates.getLast()))) {//this replica has pending updates and its last pending upd is not more recent than the one inside the election msg
        newElectionmsg = msg;//just forward election msg

      }else if(msg.lastUpdate.equals(pendingUpdates.getLast())) {//last pending update equals to the one in this replica,add itself between best candidates
        ArrayList<ActorRef> lastUpdateHolders = msg.lastUpdateHolders;
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(pendingUpdates.getLast(), lastUpdateHolders);

      }else { //my update is more recent one, update election msg before forwarding
        ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(pendingUpdates.getLast(), lastUpdateHolders);
      }

      sendMessage(receivingReplica, newElectionmsg);

      timerElectionMsg = initTimeout(TIMEOUT_MSG, TIMEOUT_MSG,new ElectionACKTimeout(receivingReplica, newElectionmsg));

    }
  }

  /*  Received ack from following replica during election process
  * */
  private void onElectionMsgACK(ElectionMsgACK msg) {
    if(crashed || !election)//don't answer to msg if state is crashed
      return;
    System.out.println("["+getSelf().path().name()+"] received ACK from " + getSender().path().name() + " during election");
    //cancel timeout for election msg ack
    timerElectionMsg.cancel();
  }

  /*  Following replica seems to have failed as well, forward to next one
  * */
  private void onElectionACKTimeout(ElectionACKTimeout msg) {
    if(crashed || !election)//don't answer to msg if state is crashed or if election is already terminated
      return;

    System.out.println("["+getSelf().path().name()+"] not received ACK from " + msg.receivingReplica.path().name() + " during election");
    //send election message to next replica wrt the one that didn't answer causing this timeout
    ActorRef receivingReplica = selectNextReplica(msg.receivingReplica, true);
    sendMessage(receivingReplica, msg.electionMsg);

    timerElectionMsg.cancel();
    timerElectionMsg = initTimeout(TIMEOUT_MSG, TIMEOUT_MSG,new ElectionACKTimeout(receivingReplica, msg.electionMsg));
  }

  /*  Received synchronization msg as result of convergence in election protocol
   * */
  private void onSynchronizationMsg(SynchronizationMsg msg) {
    if(crashed || !election)//don't answer to msg if state is crashed
      return;

    election = false;//election mode terminated
    System.out.println("["+getSelf().path().name()+"] received synchronization msg from " + getSender().path().name());

    if(msg.newCoordinator == getSelf()) {
      //current replica is the new coordinator
      coordinator = true;
      coordinatorRef = getSelf();
      timerHeartBeat = initTimeout(INTERVAL_HEARTBEAT, INTERVAL_HEARTBEAT, new SendHeartBeat());//as new coordinator, send heartbeats regularly

    }else{
      //update coordinator info
      coordinator = false;
      coordinatorRef = msg.newCoordinator;
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT, TIMEOUT_HEARTBEAT, new HeartBeatNotArrived());//expects heartbeats from new coordinator

    }

    //perform last pending update of current epoch, before starting a new one
    if(msg.updToBePerformed != null) {
      this.v = msg.updToBePerformed.v;
      this.clock = new LocalTime(msg.updToBePerformed.clock.epoch + 1, 0);
    }

    //cleanup pending updates of previous epochs because they're obsolete now
    pendingUpdates.clear();
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
      .match(CoordinatorTimeout.class, this::onCoordinatorTimeout)
      .match(SendHeartBeat.class, this::onSendHeartBeat)
      .match(ElectionMsg.class, this::onElectionMsg)
      .match(ElectionMsgACK.class, this::onElectionMsgACK)
      .match(ElectionACKTimeout.class, this::onElectionACKTimeout)
      .match(SynchronizationMsg.class, this::onSynchronizationMsg)
      .match(Request.class, this::onRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateACK.class, this::onUpdateACK)
      .match(WriteOK.class, this::onWriteOK).build();
  }
}
