package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
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
	private static final int KB = 1024;
	private static final int MB = KB * KB;
    private static final int CHUNK_SIZE_BYTES = MB;
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
    public static class BytesMessage<T> implements Serializable {
        //private static final long serialVersionUID = 4057807743872319842L;
        private T bytes;
        //private ActorRef sender;
        //private ActorRef receiver;
    }

    @Data @NoArgsConstructor @AllArgsConstructor
    public static class StreamCompleted {
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
		BytesMessage<?> bytesMessage = new BytesMessage<>(message.getMessage());
        messageContents = serialize(bytesMessage);

        Source<byte[], NotUsed> source = Source.from(messageContents);
        source.buffer(BUFFER_SIZE, OverflowStrategy.backpressure())
                .runWith(sink, ActorMaterializer.create(this.context().system()));
	}

	private void handle(byte[] messageChunk) {
	    this.messageChunks.add(messageChunk);
    }

	private void handle(StreamCompleted completed) {
		log().info("Stream completed with {} chunks", this.messageChunks.size());
		BytesMessage<?> message = null;
        try {
            message = deserialize(this.messageChunks);
        } catch (IOException e) {
            log().error("Could not deserialize LargeMessage for {}", completed.receiver);
            e.printStackTrace();
        } finally {
            messageChunks = new LinkedList<>();
        }

        if (message != null) {
            completed.receiver.tell(message.bytes, completed.sender);
            log().info("Sent LargeMessage of {} to {}", message.bytes.getClass(), completed.receiver);
        }
    }

	private void handle(StreamFailure failed) {
		messageChunks = new LinkedList<>();
	    log().error(failed.getCause(), "Stream failed");
	}

    private <T> List<byte[]> serialize(BytesMessage<T> message) {
        Kryo kryo = getKryo();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Output output = new Output(stream);
        kryo.writeObject(output, message);
        output.close();
        byte[] buffer = stream.toByteArray();

        try {
            stream.close();
        } catch (IOException e) {
            // ignore
        }
        List<byte[]> result = new LinkedList<>();
        for(int i = 0; i < buffer.length; i += CHUNK_SIZE_BYTES) {
            int end = Math.min(i + CHUNK_SIZE_BYTES, buffer.length);
            result.add(Arrays.copyOfRange(buffer, i, end));
        }
        return result;
    }
    private static BytesMessage<?> deserialize(List<byte[]> data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
	    for(byte[] i: data) {
	        out.write(i);
        }
        Kryo kryo = getKryo();
        BytesMessage<?> message = kryo.readObject(new Input(new ByteArrayInputStream(out.toByteArray())),
                BytesMessage.class);

        try {
            out.close();
        } catch (IOException e) {
            // ignore
        }
        return message;
    }

    private static Kryo getKryo() {
	    Kryo kryo = new Kryo();
        FieldSerializer<?> serializer = new FieldSerializer<BytesMessage<?>>(kryo, BytesMessage.class);
        kryo.register(BytesMessage.class, serializer);
        return kryo;
    }
}
