package io.anasy.example;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @PostMapping
    public Order place(@RequestBody OrderRequest request,
                       @RequestParam(defaultValue = "false") boolean wait) {
        return wait ? orders.placeAndWait(request) : orders.place(request);
    }
}
