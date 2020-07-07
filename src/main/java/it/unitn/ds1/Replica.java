package it.unitn.ds1;
import akka.actor.Props;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.Random;
import it.unitn.ds1.Message.GenericMessage;
import it.unitn.ds1.Message.Request;
import it.unitn.ds1.Message.RequestType;
import it.unitn.ds1.Message.Response;

// The Replica actor
public class Replica extends AbstractActor {
  
  private Random rnd = new Random();
  // value v hold by every replica
  private int v;
  // is the replica a coordinator
  private boolean coordinator;

  public Replica(int v, boolean coordinator){
    this.v = v;
    this.coordinator = coordinator;
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

  // Here we define our reaction on the received Hello messages
  private void onRequest(Request r) {
    ActorRef sender = r.client;
    if(r.rtype == RequestType.READ)
      sendMessage(sender, new Response(v));//return immediately local value
    
    else if(r.rtype == RequestType.WRITE)
      System.out.println("Write request");//TODO update(sender, r.v);
  }

  // Here we define the mapping between the received message types
  // and our actor methods
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Request.class, this::onRequest).build();
  }
}
