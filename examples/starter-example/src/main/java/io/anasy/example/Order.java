package io.anasy.example;

import java.util.UUID;

public record Order(UUID id, String customerName, double totalAmount) {}
