package io.anasy.example;

import java.util.UUID;

public record FatOrderEvent(String type, UUID orderId, String customerName, double totalAmount) {

    static FatOrderEvent from(Order order) {
        return new FatOrderEvent("order.placed", order.id(), order.customerName(), order.totalAmount());
    }
}
