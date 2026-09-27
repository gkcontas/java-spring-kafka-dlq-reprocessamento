package com.gkcontas.orders.web;

import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.dto.ReprocessRequest;
import com.gkcontas.orders.dto.ReprocessResultResponse;
import com.gkcontas.orders.service.DeadLetterService;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dlq")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    /** Read-only, and genuinely so: no offset is committed by this call. */
    @GetMapping
    public List<DeadLetterRecordResponse> list() {
        return deadLetterService.list();
    }

    @PostMapping("/reprocess")
    public ReprocessResultResponse reprocess(@RequestBody(required = false) ReprocessRequest request) {
        return deadLetterService.reprocess(request == null ? null : request.records());
    }

    /**
     * DELETE is a convenient verb, not an accurate one: the record stays in the log, since
     * Kafka has no way to remove one. What this does is record the decision so the listing
     * stops showing it.
     */
    @DeleteMapping("/{partition}/{offset}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void discard(@PathVariable int partition,
                        @PathVariable long offset,
                        @RequestParam(required = false) String reason) {
        deadLetterService.discard(partition, offset, reason);
    }
}
