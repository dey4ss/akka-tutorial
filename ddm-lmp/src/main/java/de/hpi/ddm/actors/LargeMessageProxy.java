package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	public static class StreamInitialized {}
	public static class StreamCompleted {}
	public enum Ack { INSTANCE }

	public static class StreamFailure {
		private final Throwable cause;

		public StreamFailure(Throwable cause) {
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(StreamInitialized.class, this::handle)
				.match(StreamCompleted.class, this::handle)
				.match(StreamFailure.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		Duration timeout = Duration.ofSeconds(5);
		ActorRef receiverProxy = null;

		while (receiverProxy == null) {
			try {
				receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME))
						.resolveOne(timeout).toCompletableFuture().get();
			} catch (InterruptedException | ExecutionException e) {
				log().error("Could not connect to ReceiverProxy.");
				e.printStackTrace();
				return;
			}
		}

		Sink<BytesMessage<?>, NotUsed> sink =
				Sink.actorRefWithAck(
						receiverProxy,
						new StreamInitialized(),
						Ack.INSTANCE,
						new StreamCompleted(),
						StreamFailure::new
				);

		BytesMessage<?> bytesMessage = new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver());
		Source<BytesMessage<?>, NotUsed> source = Source.single(bytesMessage);
		source.runWith(sink, ActorMaterializer.create(this.context().system()));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		//receiverProxy.get().tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		log().info("Received BytesMessage: {}", message);
		sender().tell(Ack.INSTANCE, self());
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}

	private void handle(StreamInitialized init) {
		log().info("Stream initialized");
		sender().tell(Ack.INSTANCE, self());
	}

	private void handle(StreamCompleted completed) {
		log().info("Stream completed");
	}

	private void handle(StreamFailure failed) {
		log().error(failed.getCause(), "Stream failed");
	}
}
