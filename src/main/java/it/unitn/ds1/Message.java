package it.unitn.ds1;
import akka.actor.Actor;
import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.List;
import java.util.Collections;

public abstract class Message{

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

}