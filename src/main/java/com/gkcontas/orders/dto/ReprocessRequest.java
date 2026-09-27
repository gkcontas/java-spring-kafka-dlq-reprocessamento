package com.gkcontas.orders.dto;

import java.util.List;

/** @param records the records to reprocess; empty or absent means "every outstanding one". */
public record ReprocessRequest(List<RecordCoordinates> records) {
}
