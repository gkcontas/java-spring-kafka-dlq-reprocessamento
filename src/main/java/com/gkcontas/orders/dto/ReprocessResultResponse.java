package com.gkcontas.orders.dto;

import java.util.List;

public record ReprocessResultResponse(
        int republished,
        int discarded,
        List<String> notes) {
}
