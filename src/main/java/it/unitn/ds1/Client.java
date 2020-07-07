package it.unitn.ds1;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import java.util.List;
import akka.actor.Cancellable;
import java.util.Random;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import it.unitn.ds1.Message.GenericMessage;
import it.unitn.ds1.Message.Request;
import it.unitn.ds1.Message.RequestType;
import it.unitn.ds1.Message.Response;

// The Client actor
public class Client extends AbstractActor {
  
  private Random rnd = new Random();
  // replicas that hold value v to be read and/or modified
  private List<ActorRef> replicas;

  public Client(List<ActorRef> replicas) {
    this.replicas = replicas;
  }

  static public Props props(List<ActorRef> replicas) {
    return Props.create(Client.class, () -> new Client(replicas));
  }

  private ActorRef selectRandomReplica(){
    int randomPos = rnd.nextInt(replicas.size());
    return replicas.get(randomPos);
  }

  @Override
  public void preStart() {
    //read scheduling
    Cancellable timerRead = getContext().system().scheduler().scheduleWithFixedDelay(
      Duration.create(1, TimeUnit.SECONDS),               // when to start generating messages
      Duration.create(rnd.nextInt(16), TimeUnit.SECONDS),               // how frequently generate them
      selectRandomReplica(),                                           // destination actor reference
      new Request(getSelf(), RequestType.READ, null), // the message to send
      getContext().system().dispatcher(),                 // system dispatcher
      getSelf()                                           // source of the message (myself)
    );

    //write scheduling
    Cancellable timerWrite = getContext().system().scheduler().scheduleWithFixedDelay(
      Duration.create(rnd.nextInt(32), TimeUnit.SECONDS),               // when to start generating messages
      Duration.create(rnd.nextInt(128), TimeUnit.SECONDS),               // how frequently generate them
      selectRandomReplica(),                                           // destination actor reference
      new Request(getSelf(), RequestType.WRITE, rnd.nextInt(10)), // the message to send
      getContext().system().dispatcher(),                 // system dispatcher
      getSelf()                                           // source of the message (myself)
    );
  }

  // Here we define our reaction on the received message from the replica containing the value we're interested
  private void onResponse(Response r) {
    System.out.println("Client <" +
            getSelf().path().name() +           // the name of the current actor
            "> read done from " +
            getSender().path().name() +         // the name of the sender actor
            ", value: " + r.v                   // finally the message contents
    );
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(Response.class, this::onResponse)
      .build();
  }

}

