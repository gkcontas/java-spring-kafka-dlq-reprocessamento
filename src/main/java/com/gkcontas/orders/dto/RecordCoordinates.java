package com.gkcontas.orders.dto;

/** Where a record sits in the log. Partition plus offset is the only identity it has. */
public record RecordCoordinates(int partition, long offset) {
}
