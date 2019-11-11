package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
    private static final int CHUNK_SIZE_BYTES = 1024 * 1024;
    private static final int BUFFER_SIZE = 5;

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }
	private List<byte[]> messageChunks = new LinkedList<>();

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
    public static class StreamCompleted  implements Serializable {
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data @AllArgsConstructor
	public static class StreamFailure {
		private final Throwable cause;
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
				.match(StreamCompleted.class, this::handle)
				.match(StreamFailure.class, this::handle)
				.match(byte[].class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		Duration timeout = Duration.ofSeconds(5);
		ActorRef receiverProxy;

		try {
			receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME))
					.resolveOne(timeout).toCompletableFuture().get();
		} catch (InterruptedException | ExecutionException e) {
			log().error("Could not connect to ReceiverProxy.");
			e.printStackTrace();
			return;
		}

		Sink<byte[], NotUsed> sink = Sink.actorRef(
		        receiverProxy,
                new StreamCompleted(this.sender(), receiver));
		List<byte[]> messageContents;
		try {
            messageContents = serialize(message.getMessage());
        } catch (IOException e) {
            log().warning("Could not serialize LargeMessage of type {}", message.getMessage().getClass());
            e.printStackTrace();
            return;
        }

        Source<byte[], NotUsed> source = Source.from(messageContents);
		source.buffer(BUFFER_SIZE, OverflowStrategy.backpressure())
                .runWith(sink, ActorMaterializer.create(this.context().system()));
	}

	private void handle(byte[] messageChunk) {
	    this.messageChunks.add(messageChunk);
    }

	private void handle(StreamCompleted completed) {
		log().info("Stream completed with {} chunks", this.messageChunks.size());
		Object message = null;
        try {
            message = deserialize(this.messageChunks);
        } catch (IOException | ClassNotFoundException e) {
            log().error("Could not deserialize LargeMessage to {}", completed.receiver);
            e.printStackTrace();
        } finally {
            messageChunks = new LinkedList<>();
        }

        if (message != null) {
            completed.receiver.tell(message, completed.sender);
            log().info("Sent LargeMessage of {} to {}", message.getClass(), completed.receiver);
        }
    }

	private void handle(StreamFailure failed) {
		log().error(failed.getCause(), "Stream failed");
	}

    private static List<byte[]> serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        byte[] bytes = out.toByteArray();
        out.close();
        os.close();
        List<byte[]> result = new LinkedList<>();
        for(int i = 0; i < bytes.length; i += CHUNK_SIZE_BYTES) {
            result.add(Arrays.copyOfRange(bytes, i, i + CHUNK_SIZE_BYTES));
        }
        return result;
    }
    private static Object deserialize(List<byte[]> data) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
	    for(byte[] i: data) {
	        out.write(i);
        }
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ObjectInputStream is = new ObjectInputStream(in);
        Object result = is.readObject();
        in.close();
        is.close();
        out.close();
        return result;
    }
}
