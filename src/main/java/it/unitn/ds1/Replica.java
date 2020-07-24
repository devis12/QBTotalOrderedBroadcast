package it.unitn.ds1;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;

// The Replica actor
public class Replica extends AbstractActor {

  private static final String logFolderPath = "logs";

  //interval to generate heartbeat (coordinator) (increase it proportional to num of tot replicas)
  private static int INTERVAL_HEARTBEAT_UNIT = 512;
  //interval at which replica expects heartbeat (increase it proportional to num of tot replicas)
  private static int TIMEOUT_HEARTBEAT_UNIT = 1024;

  //Timeout when expepecting messages in response from coordinator (increase it proportional to num of tot replicas)
  private static int TIMEOUT_MSG_UNIT = 512;
  //Max time allowed for link communication delays
  private static int MAX_NETWORK_DELAY = 16;

  public static class GenericMessage implements Serializable{}

  //heartbeat msg sent by the coordinator
  public static class CoordinatorHeartBeat extends GenericMessage{}
  //coordinator notify itself that it is time to spawn a heartbeat msgs
  public static class SendHeartBeat extends GenericMessage{}
  //replicas notify themselves about the coordinator heartbeat not arrived
  public static class HeartBeatNotArrived extends GenericMessage{}
  //replicas notify themselves about the coordinator not replying when it is supposed to (i.e. it's crashed)
  public static class CoordinatorTimeout extends GenericMessage{
    private final Update update;
    public CoordinatorTimeout(Update update){
      this.update = update;
    }
  }
  //msg for timeout when election has somehow failed
  public static class ElectionFailedMsg extends GenericMessage{}

  public static enum CrashStatus {
    FALSE, //NOT crashed: for replicas who's correctly working
    CRASHED, //CRASHED: replicas not responding, neither sending any messages
    BEFORE_UPDATE, //has to crash before sending update / processing update
    UPDATE, //has to crash while sending updates (for coordinator)
    AFTER_UPDATE, //has to crash after sending updates / processing update
    BEFORE_WRITEOK, //has to crash before sending writeok / processing writeok
    WRITEOK, //has to crash while sending writeok (for coordinator)
    AFTER_WRITEOK, //has to crash after sending writeok / processing writeok
    BEFORE_REQUEST, //crashed just before processing request (for common replica)
    AFTER_REQUEST, //crashed just after processing request (for common replica)
    BEFORE_UPDATEACK,//crashed just before processing updateack (for coordinator)
    AFTER_UPDATEACK, //crashed just after processing updateack (for coordinator)
    BEFORE_ELECTION, //crashed just after processing election msg (for replica)
    AFTER_ELECTION //crashed just after processing election msg (for replica)
  };
  /*tells a replica (or the coordinator) to enter crash mode before doing a specific action (if specified)
  FALSE: the replica is still active | GENERAL: crash immediately | UDPDATE: crash the next time you have to forward an UPDATE msg 
  WRITEOK: crash the next time you have to send the WRITEOK msg | and so on...
  */
  public static class CrashMsg extends GenericMessage{
    public final CrashStatus ctype;
    
    public CrashMsg(CrashStatus ctype){
      this.ctype = ctype;
    }
  }
  
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
      Client request for read or write request which will be send to a replica
    */
    public Request(ActorRef client, RequestType rtype, Integer v) {
      this.client = client;
      this.rtype = rtype;
      this.v = v;//the value in case rtype==READ is the idRead of the client
    }
  }

  // This class represents a message response (r) from a client
  public static class Response extends GenericMessage{
    public final Integer v;
    public final Integer idRead;
    public Response(Integer v, Integer idRead) {
      this.v = v;
      this.idRead = idRead;
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
      return (this.v == upd2.v && this.clock.equals(upd2.clock));
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 53 * hash + this.clock.hashCode();
      hash = 53 * hash + this.v;
      return hash;
    }
  }

  /*Update object that can be packed with an OK flag to denote the fact that the WRITEOK has been already arrived for that update*/
  public static class UpdateOK extends Update{
    private boolean okToDeliver;
    private UpdateOK(LocalTime clock, Integer v, boolean okToDeliver){
      super(clock, v);
      this.okToDeliver = okToDeliver;
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
    public final int electionMsgID;
    public final int electionEpoch;
    public final Update lastUpdate;
    public final ArrayList<ActorRef> lastUpdateHolders;
    public ElectionMsg(int electionMsgID, int electionEpoch, Update lastUpdate, ArrayList<ActorRef> lastUpdateHolders){
      this.electionMsgID = electionMsgID;
      this.electionEpoch = electionEpoch;
      this.lastUpdate = lastUpdate;
      this.lastUpdateHolders = lastUpdateHolders;
    }
  }

  // Election message ack to be sent to next replica up
  public static class ElectionMsgACK extends GenericMessage{
    public final int electionMsgID;
    public ElectionMsgACK(int electionMsgID){
      this.electionMsgID = electionMsgID;
    }
  }

  // Election message to be sent to next replica up
  public static class ElectionMsgACKTimeout extends GenericMessage{
    public final ActorRef receivingReplica;
    public final ElectionMsg electionMsg;
    public ElectionMsgACKTimeout(ActorRef receivingReplica, ElectionMsg electionMsg){
      this.receivingReplica = receivingReplica;
      this.electionMsg = electionMsg;
    }
  }

  // Synchronization message to terminate election
  public static class SynchronizationMsg extends GenericMessage{
    public final int electionEpoch;
    public final ActorRef newCoordinator;
    public final ArrayDeque<Update> updsToBePerformed;
    public SynchronizationMsg(int electionEpoch, ActorRef newCoordinator, ArrayDeque<Update> updsToBePerformed){
      this.electionEpoch = electionEpoch;
      this.newCoordinator = newCoordinator;
      this.updsToBePerformed = updsToBePerformed;
    }
  }

  /*  Custom class to contain ActorRef and a boolean to denote its state: active or faulty */
  private class ReplicaRef {
    private ActorRef ref;
    private boolean active;
    private ReplicaRef(ActorRef ref, boolean active){
      this.active = active;
      this.ref = ref;
    }
  }


  // replicas that hold value v to be read and/or modified
  private List<ReplicaRef> replicas;
  // value v hold by every replica
  private int v;
  // coordinator reference
  private ActorRef coordinatorRef;
  // timer for handling coordinator heartbeat (e.g. keep alive msg)
  private Cancellable timerHeartBeat;
  // timer for handling coordinator's failure when replica timeouts waiting for the writeOK
  private HashMap<Update, Cancellable> timersUpdateACK = new HashMap<>();
  // to timeout during election process if not receiving ACK from following replica (id associated to election msgs)
  private HashMap<Integer, Cancellable> timersElectionMsg = new HashMap<>();
  // to relate an id to every new election msg within an election epoch
  private int electionMsgCounter;
  // to timeout during election process if election doesn't come to an end (e.g. caused by crashing of neighbouring nodes)
  private Cancellable timerElectionFailed;
  // is the replica a coordinator
  private boolean coordinator;
  //hashmap containing number of ack received for a given update 
  private HashMap<LocalTime, Integer> ackMap = new HashMap<> ();
  //stack for pending updates
  private ArrayDeque<UpdateOK> pendingUpdates = new ArrayDeque<>();
  //local time wrt this replica, which consists in epoch+sequence number
  private LocalTime clock;
  //has the replica crashed? (general crash)
  private CrashStatus crashed;
  //election process in progress
  private boolean election;
  //synchronization process in progress
  private boolean synchronization;

  public Replica(int v, boolean coordinator){
    this.v = v;
    this.coordinator = coordinator;
    clock = new LocalTime(0,0);
    crashed = CrashStatus.FALSE; //by default the replica is active (in a non-crashed state)
    election = false;
    synchronization = false;
  }

  // Some stuff required by Akka to create actors of this type
  static public Props props(int v, boolean coordinator) {
    return Props.create(Replica.class, () -> new Replica(v, coordinator));
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

  private void sendMessage(ActorRef receiver, GenericMessage msg){
    if(crashed == CrashStatus.CRASHED) //don't answer to msg if state is crashed
      return;

    if(msg.getClass() ==  Response.class){

      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] sending Response msg " +
              ((Response)msg).v + " to " + receiver.path().name());
    }
    if(msg.getClass() ==  Update.class){

      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] sending UPDATE msg " + ((Update)msg).v + " " +
              ((Update)msg).clock + " to " + receiver.path().name());
    }
    if(msg.getClass() ==  UpdateACK.class){

      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] sending UPDATEACK msg " + ((UpdateACK)msg).v + " " +
              ((UpdateACK)msg).clock + " to " + receiver.path().name());
    }
    if(msg.getClass() == WriteOK.class){

      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] sending WRITEOK msg " + ((WriteOK)msg).v + " " +
              ((WriteOK)msg).clock + " to " + receiver.path().name());
    }
    //try{Thread.sleep(2000);}catch(Exception e){}

    receiver.tell(msg, getSelf());

    // simulate network delays using sleep
    try { Thread.sleep(new Random(System.currentTimeMillis()).nextInt(MAX_NETWORK_DELAY)); }
    catch (InterruptedException e) { e.printStackTrace(); }

  }
  /*  If broadcast equals true send also to itself
  * */
  private void multicastMessageToReplicas(GenericMessage msg, boolean broadcast){
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    if(msg.getClass() == Update.class){

      if(crashed == CrashStatus.BEFORE_UPDATE){//coordinator is asked to crash before update
        crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
        System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_UPDATE");
        return;
      }

      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] multicasting UPDATE msg " + ((Update)msg).v + " " +
              ((Update)msg).clock);
    }
    if(msg.getClass() == WriteOK.class){
      if(crashed == CrashStatus.BEFORE_WRITEOK){//coordinator is asked to crash before writeOK
        crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
        System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_WRITEOK");
        return;
      }
      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] multicasting WRITEOK msg " + ((WriteOK)msg).v + " " +
              ((WriteOK)msg).clock);
    }
    
    // randomly arrange replicas
    List<ActorRef> shuffledGroup = new ArrayList<>();
    for(ReplicaRef replica : this.replicas)
      shuffledGroup.add(replica.ref);
    Collections.shuffle(shuffledGroup);

    // multicast to all peers in the group
    for (ActorRef r: shuffledGroup) {
      if(isFaulty(r))//avoid sending messages to replicas which you know are faulty
        continue;

      if (broadcast || !r.equals(getSelf()))//avoid sending to yourself in multicast
        sendMessage(r, msg);

      if(msg.getClass() == Update.class && crashed == CrashStatus.UPDATE){//crash randomly during update)
        if(new Random(System.currentTimeMillis()).nextInt(2) % 2 == 0){
          crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
          System.err.println("[" + getSelf().path().name() + "] CRASHED UPDATE");
          return;
        }
      }

      if(msg.getClass() == WriteOK.class && crashed == CrashStatus.WRITEOK){//crash randomly during update)
        if(new Random(System.currentTimeMillis()).nextInt(2) % 2 == 0){
          crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
          System.err.println("[" + getSelf().path().name() + "] CRASHED WRITEOK");
          return;
        }
      }

    }

    //coordinator is asked to crash after update, if it hasn't crashed during the update, make it crash
    if(msg.getClass() == Update.class && (crashed == CrashStatus.AFTER_UPDATE || crashed == CrashStatus.UPDATE)) {
      crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_UPDATE");
    }
    
    //coordinator is asked to crash after writeok, if it hasn't crashed during the writeok sending, make it crash
    if(msg.getClass() == WriteOK.class && (crashed == CrashStatus.AFTER_WRITEOK || crashed == CrashStatus.WRITEOK)) {
      crashed = CrashStatus.CRASHED; //after specific crash, transform it into a general one
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_WRITEOK");
    }

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
  private boolean canDeliver(UpdateOK upd) {
    if(!upd.okToDeliver)//WRITEOK not arrived for this update
      return false;

    for(Update pupd : pendingUpdates)
      if(pupd.clock.epoch < upd.clock.epoch || pupd.clock.sn < upd.clock.sn)
        return false;

    return true;
  }

  /*  Return a pending update that can be delivered, otherwise null
   * */
  private UpdateOK canDeliverSomething() {
    for(UpdateOK pupd : pendingUpdates)
      if(canDeliver(pupd))
        return pupd;

    return null;
  }

  private void deliver(Update upd){
    //perforn update
    this.v = upd.v;
    System.out.println("[" + getSelf().path().name() + " " + this.clock + "]" +
            " update " + upd.clock + " " + upd.v);

    //write also in log file //Replica <ReplicaID> update <e>:<i> <value>
    appendLog("Replica " + getSelf().path().name() + " update " + upd.clock + " " + upd.v);

    if(!coordinator || synchronization)//coordinator updates clock when sending update, unless we're in synchronization mode
      this.clock = upd.clock;//others will update their clocks when delivering

    //delete from pending updates list
    pendingUpdates.remove(upd);
  }

  /*return true if replica is marked as failed*/
  private boolean isFaulty(ActorRef r){
    for(ReplicaRef replica : this.replicas)
      if(replica.ref.equals(r))
        return !replica.active;

    return true;//it has failed so bad that it doesn't exist
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
      System.err.println("[" + getSelf().path().name() + " " + this.clock + "] IOException while writing in logfile " + Paths.get(getSelf().path().name()+"_log.txt").toAbsolutePath());
    }
  }

  /*  Discriminate maximum replica by considering their ids, eg "name1", "name2" where name2 is the maximum
  * */
  private boolean isMaxById(ArrayList<ActorRef> lastUpdateHolders, String prefix){
    int selfId = Integer.parseInt(getSelf().path().name().substring(prefix.length()));
    for(ActorRef actor : lastUpdateHolders) {
      String name = actor.path().name();
      if (Integer.parseInt(name.substring(prefix.length())) > selfId)
        return false;
    }
    return true;
  }

  /*  Select in the list of replicas the following NON_FAULTY replica wrt current replica
  *   with an option to skip the coordinator ActorRef (useful in case of election)
  * */
  private ActorRef selectNextReplica(ActorRef rep, boolean skipCoordinator){
    int myIndex = -1;//position of the current replica in the ring
    for(int i=0; i<replicas.size();i++)
      if(replicas.get(i).ref == rep)
        myIndex = i;

    int selectIndex = (myIndex + 1 == replicas.size())? 0 : myIndex + 1; //select next index
    while(!replicas.get(selectIndex).active) { //select a replica that you know as active and non-faulty
      selectIndex = (selectIndex + 1 == replicas.size()) ? 0 : selectIndex + 1;

      if(skipCoordinator && replicas.get(selectIndex).ref == coordinatorRef) //in case you want to skip coordinator
        selectIndex = (selectIndex + 1 == replicas.size()) ? 0 : selectIndex + 1;
    }

    return replicas.get(selectIndex).ref;
  }

  /*  Start election process after coordinator's failure detection
  * */
  private void initiateElection() {
    if(election)//to avoid to init election, when one is already running
      return;

    electionMsgCounter = 1;

    cancelPendingUpdTimers();
    timerHeartBeat.cancel();//coordinator is going to change and timer for heartbeat is going to be reinit when new coordinator is chosen
    //election is supposed to end after a while, otherwise something bad has happened
    timerElectionFailed = initTimeout((TIMEOUT_MSG_UNIT*replicas.size())*replicas.size(),(TIMEOUT_MSG_UNIT*replicas.size())*replicas.size(), new ElectionFailedMsg());

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] initiating election");
    election = true;
    //send election message
    ActorRef receivingReplica = selectNextReplica(getSelf(), true);
    ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
    lastUpdateHolders.add(getSelf());
    ElectionMsg newElectionMsg = null;
    if(pendingUpdates.size() > 0)//pick your last update to forward on if there is any
      newElectionMsg = new ElectionMsg(electionMsgCounter, clock.epoch, pendingUpdates.getLast(), lastUpdateHolders);
    else
      newElectionMsg = new ElectionMsg(electionMsgCounter, clock.epoch,null, lastUpdateHolders);
    sendMessage(receivingReplica, newElectionMsg);

    timersElectionMsg.put(electionMsgCounter, initTimeout(TIMEOUT_MSG_UNIT * replicas.size(), TIMEOUT_MSG_UNIT * replicas.size(),new ElectionMsgACKTimeout(receivingReplica, newElectionMsg)));
  }

  /*  Terminate election process by broadcasting to all, that youp're the new coordinator
  * */
  private void synchronization(){
    if(synchronization)//already in synch mode
      return;
    synchronization = true;
    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] initiating synchronization to terminate election");

    //Convert to list of simple plain Update objects (update ok contains a mutable variable)
    ArrayDeque<Update> pendingUpdatestoSend = new ArrayDeque<>();
    for(UpdateOK updOk : pendingUpdates)
      pendingUpdatestoSend.addLast(new Update(updOk.clock, updOk.v));

    multicastMessageToReplicas(new SynchronizationMsg(this.clock.epoch, getSelf(), pendingUpdatestoSend), true);
  }

  /* delete all pending updates' timers
  */
  private void cancelPendingUpdTimers(){
    for(Update upd : timersUpdateACK.keySet())
      timersUpdateACK.get(upd).cancel();
    
    timersUpdateACK.clear();
  }

  /* delete all election msg ack timers
   */
  private void cancelElectionMsgACKTimers(){
    for(Integer elMsgID : timersElectionMsg.keySet())
      timersElectionMsg.get(elMsgID).cancel();

    timersElectionMsg.clear();
  }

  /*mark the coordinator as failed*/
  private void markCoordinatorFailed(){
    for(ReplicaRef replica : this.replicas)
      if(replica.ref.equals(coordinatorRef))
        replica.active = false;
  }

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    this.replicas = new ArrayList<>();
    for(ActorRef r : msg.group)
      replicas.add(new ReplicaRef(r, true));//at the start we assume all replicas are up and running (i.e. not faulty)

    this.coordinatorRef = msg.coordinatorRef;

    System.out.println("Starting heartbeat process");
    if(coordinator)
      timerHeartBeat = initTimeout(INTERVAL_HEARTBEAT_UNIT * replicas.size(), INTERVAL_HEARTBEAT_UNIT * replicas.size(), new SendHeartBeat());

    else
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT_UNIT * replicas.size(), TIMEOUT_HEARTBEAT_UNIT * replicas.size(), new HeartBeatNotArrived());
  }

  /*  Need to enter in crashed mode
  * */
  private void onCrashMsg(CrashMsg cmsg) {
    if(cmsg.ctype == CrashStatus.CRASHED)
      System.err.println("[" + getSelf().path().name() + "] CRASHED ON MSG CRASHED");

    crashed = cmsg.ctype;
  }

  // Here we define our reaction on the received Request (read/write) messages
  private void onRequest(Request r) {
    if(crashed == CrashStatus.CRASHED)
      return;

    if(crashed == CrashStatus.BEFORE_REQUEST) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_REQUEST");
      return;
    }

    ActorRef sender = r.client;
    if(r.rtype == RequestType.READ) {
      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received read request from client " + getSender().path().name() + " with readId " + r.v);
      sendMessage(sender, new Response(v, r.v));//return immediately local value  (value sent by client is the readId in case of read request)

    }else if(r.rtype == RequestType.WRITE && !coordinator) {//forward write request to coordinator (if election is not currently running)
      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received write request from client " + getSender().path().name());
      if(!election)// (if election is not currently running)
        sendMessage(coordinatorRef, r);//forward request to coordinator in normal scenario (aka if the coordinator hasn't crashed)

    }else if(r.rtype == RequestType.WRITE && coordinator){//I'm the coordinator, tell other replicas about the update
      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received write request from client " + getSender().path().name());
      this.clock = LocalTime.newSequence(clock);//update clock
      multicastMessageToReplicas(new Update(clock, r.v), true);
    }

    if(crashed == CrashStatus.AFTER_REQUEST) {//state has to be crashed NOW
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_REQUEST");
    }

  }

  // Here we define our reaction on the received Update messages
  private void onUpdate(Update upd) {
    if(crashed == CrashStatus.CRASHED)
      return;
    if(crashed == CrashStatus.BEFORE_UPDATE) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_UPDATE");
      return;
    }

    pendingUpdates.addLast(new UpdateOK(upd.clock, upd.v, false));//add to pending updates to be performed
    for(Update pupd:pendingUpdates)
      System.out.println("[" + getSelf().path().name() + " " + this.clock + "] pending update: " + pupd.v + " " + pupd.clock);

    sendMessage(coordinatorRef, new UpdateACK(upd.clock, upd.v));
    //start timer to detect coordinator failure while waiting for writeOK of this update
    timersUpdateACK.put(upd, initTimeout(TIMEOUT_MSG_UNIT * replicas.size(), TIMEOUT_MSG_UNIT * replicas.size(), new CoordinatorTimeout(upd)));

    if(crashed == CrashStatus.AFTER_UPDATE) {//state has to be crashed NOW
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_UPDATE");
    }
  }

  // Here we define our reaction on the received UpdateACK messages
  private void onUpdateACK(UpdateACK upd) {
    if(crashed == CrashStatus.CRASHED)
      return;
    if(crashed == CrashStatus.BEFORE_UPDATEACK) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_UPDATEACK");
      return;
    }

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
    Update updToConfirm = Update.toUpdate(upd);
    if(acksReceived >= quorum && timersUpdateACK.get(updToConfirm) != null){//WRITEOK
      timersUpdateACK.get(updToConfirm).cancel();//coordinator deleting its timer for this update
      timersUpdateACK.remove(updToConfirm);
      multicastMessageToReplicas(new WriteOK(upd.clock, upd.v), true);
    }

    if(crashed == CrashStatus.AFTER_UPDATEACK) {//state has to be crashed NOW
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_UPDATEACK");
    }
  }

  // Here we define our reaction on the received WriteOK messages, i.e. perform update
  private void onWriteOK(WriteOK wokUpd) {
    if(crashed == CrashStatus.CRASHED)
      return;
    if(crashed == CrashStatus.BEFORE_WRITEOK) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_WRITEOK");
      return;
    }

    Update writeOKUpdate = Update.toUpdate(wokUpd);
    UpdateOK confirmedUpdate = new UpdateOK(writeOKUpdate.clock, writeOKUpdate.v, true);//this update will be ok to deliver when the others are also arrived

    //cancel timeout handler for this update
    if(!coordinator) {//coordinator has already deleted this timer when
      timersUpdateACK.get(confirmedUpdate).cancel();
      timersUpdateACK.remove(confirmedUpdate);
    }

    if(canDeliver(confirmedUpdate)){//check if wokupd can be performed

      deliver(confirmedUpdate);
      //check if other pending updates can be delivered? no need... do not assume writeOK will be arrived in right order
      while((confirmedUpdate = canDeliverSomething()) != null)
        deliver(confirmedUpdate);

    }else{//ensure that in the list of pendingUpdate it appears with WRITEOK flag set to true
      for(UpdateOK updOK : pendingUpdates)
        if(updOK.equals(confirmedUpdate))
          updOK.okToDeliver = true;
    }

    if(crashed == CrashStatus.AFTER_WRITEOK) {//state has to be crashed NOW
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_WRITEOK");
    }

  }

  /*  Coordinator heartbeat to signal the fact that it is still up
  * */
  private void onCoordinatorHeartBeat(CoordinatorHeartBeat msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received heartbeat from coordinator");

    if(!coordinator){
      timerHeartBeat.cancel();
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT_UNIT * replicas.size(), TIMEOUT_HEARTBEAT_UNIT * replicas.size(), new HeartBeatNotArrived());
    }

  }

  /*  replicas notify themselves about the coordinator heartbeat not arrived
   * */
  private void onHeartBeatNotArrived(HeartBeatNotArrived msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    cancelPendingUpdTimers();

    System.err.println("[" + getSelf().path().name() + " " + this.clock + "] FAILURE OF THE COORDINATOR: heartbeat not arrived");
    timerHeartBeat.cancel();
    markCoordinatorFailed();//now I know coordinator has failed
    initiateElection();//start election process
  }

  /*  replicas notify themselves about the coordinator not replying when it is supposed to (i.e. it's crashed)
   * */
  private void onCoordinatorTimeout(CoordinatorTimeout msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    cancelPendingUpdTimers();
    if(!election) {
      System.err.println("[" + getSelf().path().name() + " " + this.clock + "] FAILURE OF THE COORDINATOR: reply not arrived after sending updateACK for " + msg.update.v + " - " + msg.update.clock);
      markCoordinatorFailed();
      initiateElection();//start election process
    }
  }

  /*  Coordinator heartbeat to signal the fact that it is still up
   * */
  private void onSendHeartBeat(SendHeartBeat msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;
    System.out.println("[" + getSelf().path().name() + " " + this.clock + "], aka coordinator sending heartbeats to everyone");

    multicastMessageToReplicas(new CoordinatorHeartBeat(), false);
  }

  /* Manage incoming electionmsg
  */
  private void onElectionMsg(ElectionMsg msg) {
    if(crashed == CrashStatus.CRASHED)//don't answer to msg if state is crashed
      return;

    if(crashed == CrashStatus.BEFORE_ELECTION) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED BEFORE_ELECTION");
      return;
    }

    //if epoch in which election is running is greater (i.e. synch already done), this election msg need to be ignored
    if(clock.epoch > msg.electionEpoch)
      return;

    if(!election) {//this msg has been stating the start of the election, which is supposed to end after a while, otherwise something bad has happened
      timerHeartBeat.cancel();//coordinator is going to change and timer for heartbeat is going to be reinit when new coordinator is chosen
      cancelPendingUpdTimers();//cancel pending upd timers
      timerElectionFailed = initTimeout((TIMEOUT_MSG_UNIT * replicas.size()) * replicas.size(), (TIMEOUT_MSG_UNIT * replicas.size()) * replicas.size(), new ElectionFailedMsg());
      electionMsgCounter = 0;
      markCoordinatorFailed();//now I know coordinator has failed
    }

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received election msg from " + getSender().path().name() + "(electionEpoch:"+msg.electionEpoch+")");
    election = true;// in case this replica is not already in election mode (e.g. coordinator's timeout detected by someone else)
    electionMsgCounter++;

    sendMessage(getSender(), new ElectionMsgACK(msg.electionMsgID));//ack of election msg

    ActorRef receivingReplica = selectNextReplica(getSelf(), true);
    ElectionMsg newElectionmsg;

    if (msg.lastUpdateHolders.contains(getSelf())){
      if(isMaxById(msg.lastUpdateHolders, "replica")){ //election msg has already performed a round and this replica is the best candidate (disambiguate by id, take the maximum)
        if(!synchronization)
          synchronization();//broadcast to everyone you're the new coordinator
        
      }else{//just pass along msg, election has converged, but you're not the best candidate
        newElectionmsg = new ElectionMsg(electionMsgCounter, msg.electionEpoch, msg.lastUpdate, msg.lastUpdateHolders);
        sendMessage(receivingReplica, newElectionmsg);

        timersElectionMsg.put(electionMsgCounter, initTimeout(TIMEOUT_MSG_UNIT * replicas.size(), TIMEOUT_MSG_UNIT * replicas.size(),new ElectionMsgACKTimeout(receivingReplica, newElectionmsg)));
      }

    }else{

      if(pendingUpdates.isEmpty() && msg.lastUpdate == null) {//no pending update is circulating until now and this replica hasn't any
        ArrayList<ActorRef> lastUpdateHolders = msg.lastUpdateHolders;
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(electionMsgCounter, clock.epoch, null, lastUpdateHolders);

      }else if(pendingUpdates.size() > 0 && msg.lastUpdate == null) {//mine considered to be the most recent update, since there is none in the election msg
        //NOTE considered this also as the case in which the WRITEOK has just reached some replicas and not all, thus this can be considered as an old update by other replicas
        ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(electionMsgCounter, clock.epoch, pendingUpdates.getLast(), lastUpdateHolders);

      }else if(pendingUpdates.isEmpty() || (msg.lastUpdate.moreRecent(pendingUpdates.getLast()))) {//this replica has pending updates (or not) and its last pending upd is not more recent than the one inside the election msg
        newElectionmsg = new ElectionMsg(electionMsgCounter, msg.electionEpoch, msg.lastUpdate, msg.lastUpdateHolders);//just forward election msg

      }else if(msg.lastUpdate.equals(pendingUpdates.getLast())) {//last pending update equals to the one in this replica,add itself between best candidates
        ArrayList<ActorRef> lastUpdateHolders = msg.lastUpdateHolders;
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(electionMsgCounter, clock.epoch, pendingUpdates.getLast(), lastUpdateHolders);

      }else { //my update is more recent one, update election msg before forwarding
        ArrayList<ActorRef> lastUpdateHolders = new ArrayList<>();
        lastUpdateHolders.add(getSelf());
        newElectionmsg = new ElectionMsg(electionMsgCounter, clock.epoch, pendingUpdates.getLast(), lastUpdateHolders);
      }

      sendMessage(receivingReplica, newElectionmsg);

      timersElectionMsg.put(electionMsgCounter, initTimeout(TIMEOUT_MSG_UNIT * replicas.size(), TIMEOUT_MSG_UNIT * replicas.size(),new ElectionMsgACKTimeout(receivingReplica, newElectionmsg)));

    }

    if(crashed == CrashStatus.AFTER_ELECTION) {//don't answer to msg if state is crashed or has to be crashed
      crashed = CrashStatus.CRASHED;//just say it is crashed to avoid answering/sending any future message
      System.err.println("[" + getSelf().path().name() + "] CRASHED AFTER_ELECTION");
      return;
    }

  }

  /*  Received ack from following replica during election process
  * */
  private void onElectionMsgACK(ElectionMsgACK msg) {
    if(crashed == CrashStatus.CRASHED || !election)//don't answer to msg if state is crashed or if election has came to conclusion
      return;

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received ACK from " + getSender().path().name() + " during election");

    //cancel timeout for election msg ack
    timersElectionMsg.get(msg.electionMsgID).cancel();
    timersElectionMsg.remove(msg.electionMsgID);

  }

  /*  Following replica seems to have failed as well, forward to next one
  * */
  private void onElectionMsgACKTimeout(ElectionMsgACKTimeout msg) {
    if(msg.electionMsg.electionEpoch < this.clock.epoch) {//this election msg ack was in any case obsolete
      if(timerElectionFailed != null)//make sure the timeout is not valid anymore
        timerElectionFailed.cancel();

      election = false;
    }

    if(crashed == CrashStatus.CRASHED || !election)//don't answer to msg if state is crashed or if election has came to conclusion
      return;

   if(timersElectionMsg.get(msg.electionMsg.electionMsgID) == null)//timer's already been cancelled and dealt with
      return;

    //now I know that this replica has failed, so I have to mark it accordingly
    for(ReplicaRef replica : this.replicas)
      if(replica.ref.equals(msg.receivingReplica))
        replica.active = false;
    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] not received ACK from " + msg.receivingReplica.path().name() + " during election");
    //send election message to next replica wrt the one that didn't answer causing this timeout
    ActorRef receivingReplica = selectNextReplica(msg.receivingReplica, true);
    electionMsgCounter++;
    sendMessage(receivingReplica, new ElectionMsg(electionMsgCounter, msg.electionMsg.electionEpoch, msg.electionMsg.lastUpdate, msg.electionMsg.lastUpdateHolders));

    timersElectionMsg.get(msg.electionMsg.electionMsgID).cancel();
    timersElectionMsg.remove(msg.electionMsg.electionMsgID);
    timersElectionMsg.put(electionMsgCounter, initTimeout(TIMEOUT_MSG_UNIT * replicas.size(), TIMEOUT_MSG_UNIT * replicas.size(),new ElectionMsgACKTimeout(receivingReplica, msg.electionMsg)));

  }

  /*  Received synchronization msg as result of convergence in election protocol
   * */
  private void onSynchronizationMsg(SynchronizationMsg msg) {
    if(crashed == CrashStatus.CRASHED || !election)//don't answer to msg if state is crashed or if election has came to conclusion
      return;

    if(msg.electionEpoch < this.clock.epoch) {//this election msg ack was in any case obsolete
      if(timerElectionFailed != null)//make sure the timeout is not valid anymore
        timerElectionFailed.cancel();

      election = false;
    }

    if(crashed == CrashStatus.CRASHED || !election)//don't answer to msg if state is crashed or if election has came to conclusion
      return;

    timerElectionFailed.cancel();//election has not failed
    cancelElectionMsgACKTimers();//you're not waiting for any election msg anymore
    election = false;//election mode terminated

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] received synchronization msg from " + getSender().path().name() +
            " with " + msg.updsToBePerformed.size() + " updates to be performed");

    if(msg.newCoordinator == getSelf()) {
      //current replica is the new coordinator
      coordinator = true;
      coordinatorRef = getSelf();
      timerHeartBeat = initTimeout(INTERVAL_HEARTBEAT_UNIT * replicas.size(), INTERVAL_HEARTBEAT_UNIT * replicas.size(), new SendHeartBeat());//as new coordinator, send heartbeats regularly

    }else{
      //update coordinator info
      coordinator = false;
      coordinatorRef = msg.newCoordinator;
      timerHeartBeat = initTimeout(TIMEOUT_HEARTBEAT_UNIT * replicas.size(), TIMEOUT_HEARTBEAT_UNIT * replicas.size(), new HeartBeatNotArrived());//expects heartbeats from new coordinator

    }

    //perform last pending update of current epoch, before starting a new one
    if(msg.updsToBePerformed != null) {

      for(Update upd : pendingUpdates)//deliver all pending updates
        deliver(upd);

      for(Update upd : msg.updsToBePerformed)//perform all pending updates which the new coordinator has received notification of, while this replica hasn't
        if (upd.clock.sn > this.clock.sn)//do not deliver twice the same update: happens if the coordinator has crashed while sending writeOK of some previous update, it can be that this replica has already performed it
          deliver(upd);

    }

    //cleanup pending updates of previous epochs because they're obsolete now
    pendingUpdates.clear();

    //update clock
    this.clock = new LocalTime(msg.electionEpoch + 1, 0);

    synchronization = false;//synchronization mode is came to an end
  }

  /* Manage election failure
   */
  private void onElectionFailedMsg(ElectionFailedMsg msg) {
    timerElectionFailed.cancel();

    if(crashed == CrashStatus.CRASHED || !election)//don't answer to msg if state is crashed or if election has came to conclusion
      return;

    System.out.println("[" + getSelf().path().name() + " " + this.clock + "] ELECTION FAILURE! Restart election ");
    election = false;
    initiateElection();//restart new election
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
      .match(ElectionMsgACKTimeout.class, this::onElectionMsgACKTimeout)
      .match(SynchronizationMsg.class, this::onSynchronizationMsg)
      .match(ElectionFailedMsg.class, this::onElectionFailedMsg)
      .match(Request.class, this::onRequest)
      .match(Update.class, this::onUpdate)
      .match(UpdateACK.class, this::onUpdateACK)
      .match(WriteOK.class, this::onWriteOK).build();
  }
}
