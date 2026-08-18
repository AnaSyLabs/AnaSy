package io.anasy.connector;

public interface EventPublisher {

    /** Non-blocking. Queues the send and returns. Failures are logged. */
    void publish(String topic, Object payload);

    /** Non-blocking with a stable eventId. Use this when the caller already has one. */
    void publish(String topic, String eventId, Object payload);

    /** Blocking. Waits for broker ack. Throws if the send fails or is interrupted. */
    void publishAndWait(String topic, Object payload);

    /** Blocking with a stable eventId. */
    void publishAndWait(String topic, String eventId, Object payload);
}
