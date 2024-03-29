package it.unitn.ds1;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import it.unitn.ds1.Replica.JoinGroupMsg;
import it.unitn.ds1.Replica.CrashMsg;
import it.unitn.ds1.Replica.CrashStatus;
import it.unitn.ds1.Client.SendReadRequest;
import it.unitn.ds1.Client.SendWriteRequest;


public class QBTotalOrderBroadcast {

  final static int N_REPLICAS = 7;
  final static int N_CLIENTS = 2;
  final static int INIT_V = 16;

  public static void main(String[] args) throws InterruptedException {

    List<ActorRef> replicas = new ArrayList<ActorRef>(); //list of available replicas
    List<ActorRef> clients = new ArrayList<ActorRef>(); //list of available clients
    ActorRef initCoordinator = null; //initial coordinator

    // Create an actor system named "qbtotorderbroad"
    final ActorSystem system = ActorSystem.create("qbtotorderbroad");

    // Create multiple Replicas actors that will retain a copy of the same variable v
    for (int i=0; i<N_REPLICAS; i++) {
      ActorRef r = system.actorOf(
        Replica.props(new Integer(INIT_V), i == N_REPLICAS-1), // specifying the initial value
        "replica" + i);    // the new actor name (unique within the system)
      
      replicas.add(r);
      if(i == N_REPLICAS-1)
        initCoordinator = r;
    }

    // ensure that no one can modify the group of replicas
    replicas = Collections.unmodifiableList(replicas);

    // send the group member list to everyone in the group of replicas
    JoinGroupMsg join = new JoinGroupMsg(replicas, initCoordinator);
    for (ActorRef peer: replicas) {
      peer.tell(join, null);
    }

    // Create multiple Client actors that will contact any replicas for read/write operations on variable v
    for (int i=0; i<N_CLIENTS; i++) {
      ActorRef c = system.actorOf(
          Client.props(replicas), 
          "client" + i);    // the new actor name (unique within the system)
      clients.add(c);
    }

    /*
       ###############################################
       #           ACTIONS CONTROL LIST              #
       ###############################################
    */

    replicas.get(3).tell(new CrashMsg(CrashStatus.AFTER_UPDATE), null);
    replicas.get(5).tell(new CrashMsg(CrashStatus.BEFORE_ELECTION), null);
    clients.get(0).tell(new SendWriteRequest(4), null);
    clients.get(1).tell(new SendWriteRequest(8), null);
    clients.get(0).tell(new SendReadRequest(), null);

    inputContinue();
    replicas.get(6).tell(new CrashMsg(CrashStatus.WRITEOK), null);
    clients.get(0).tell(new SendReadRequest(), null);
    clients.get(0).tell(new SendWriteRequest(12), null);

    inputContinue();
    clients.get(0).tell(new SendReadRequest(), null);
    clients.get(0).tell(new SendWriteRequest(32), null);

    inputContinue();

    // system shutdown
    system.terminate();
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    }
    catch (IOException ignored) {}
  }
}
