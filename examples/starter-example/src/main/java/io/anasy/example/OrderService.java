package io.anasy.example;

import io.anasy.connector.EventPublisher;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    static final String TOPIC = "orders.events";

    private final Map<UUID, Order> orders = new ConcurrentHashMap<>();
    private final EventPublisher events;

    public OrderService(EventPublisher events) {
        this.events = events;
    }

    public Order place(OrderRequest request) {
        Order saved = save(request);
        events.publish(TOPIC, saved.id().toString(), FatOrderEvent.from(saved));
        return saved;
    }

    public Order placeAndWait(OrderRequest request) {
        Order saved = save(request);
        events.publishAndWait(TOPIC, saved.id().toString(), FatOrderEvent.from(saved));
        return saved;
    }

    private Order save(OrderRequest request) {
        Order saved = new Order(UUID.randomUUID(), request.customerName(), request.totalAmount());
        orders.put(saved.id(), saved);
        return saved;
    }
}
