package it.unitn.ds1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class QBTotalOrderBroadcast {

  final static int N_REPLICAS = 4;
  final static int N_CLIENTS = 2;
  final static int INIT_V = 16;
  
  final static ArrayList<ActorRef> replicas = new ArrayList<ActorRef>();

  public static void main(String[] args) {

    // Create an actor system named "qbtotorderbroad"
    final ActorSystem system = ActorSystem.create("qbtotorderbroad");

    // Create multiple Replicas actors that will retain a copy of the same variable v
    for (int i=0; i<N_REPLICAS; i++) {
      replicas.add(
        system.actorOf(
          Replica.props(new Integer(INIT_V), i == N_REPLICAS-1), // specifying the initial value
          "replica" + i)    // the new actor name (unique within the system)
      );
    }

    // Create multiple Client actors that will contact any replicas for read/write operations on variable v
    for (int i=0; i<N_CLIENTS; i++) {
      system.actorOf(
          Client.props(replicas), 
          "client" + i);    // the new actor name (unique within the system)
    }

    System.out.println(">>> Press ENTER to exit <<<");
    try {
      System.in.read();

    }
    catch (IOException ioe) {}
    finally {
      system.terminate();
    }
  }
}
