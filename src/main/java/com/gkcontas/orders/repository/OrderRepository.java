package com.gkcontas.orders.repository;

import com.gkcontas.orders.model.Order;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

    boolean existsByEventId(String eventId);

    Optional<Order> findByEventId(String eventId);

    List<Order> findAllByOrderByIdAsc();
}
