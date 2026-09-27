package com.gkcontas.orders.model;

public enum Disposition {
    /** Republished to the main topic for another attempt. */
    REPROCESSED,
    /** Judged unrecoverable; it stays in the log but is hidden from the listing. */
    DISCARDED
}
