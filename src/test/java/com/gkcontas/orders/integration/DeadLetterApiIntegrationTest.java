package com.gkcontas.orders.integration;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.service.DemoFailureRules;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

class DeadLetterApiIntegrationTest extends IntegrationTestBase {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void shouldAcceptAnOrderWithAcceptedRatherThanCreated() throws Exception {
        // 202: this publishes an event. Whether an order comes out depends on a consumer
        // that has not run yet, and "created" would be a promise the endpoint cannot keep.
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerEmail": "api@example.com", "item": "teclado",
                                 "quantity": 2, "unitPrice": 150.00}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("PUBLISHED"));
    }

    @Test
    void shouldRejectAnInvalidPayloadBeforeItEverReachesKafka() throws Exception {
        // Validated at the edge. Publishing a malformed event instead would turn a client
        // mistake into a dead letter for somebody to triage later.
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerEmail": "not-an-email", "item": "teclado",
                                 "quantity": 1, "unitPrice": 10.00}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("customerEmail must be a valid email"));
    }

    @Test
    void shouldExposeTheDeadLetterQueueAndItsRootCause() throws Exception {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);
        awaitDeadLetter(eventId);

        mockMvc.perform(get("/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].exceptionType").isNotEmpty())
                .andExpect(jsonPath("$[0].originalTopic").value("orders"));
    }

    @Test
    void shouldReturnNotFoundWhenDiscardingSomethingThatIsNotThere() throws Exception {
        mockMvc.perform(delete("/dlq/{partition}/{offset}", 0, 999_999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Dead letter not found"));
    }

    @Test
    void shouldReprocessEverythingWhenNoSelectionIsGiven() throws Exception {
        String eventId = publish(DemoFailureRules.RECOVERING_ITEM, 1);
        DeadLetterRecordResponse deadLetter = awaitDeadLetter(eventId);

        mockMvc.perform(post("/dlq/reprocess").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.republished").exists())
                .andExpect(jsonPath("$.notes").isArray());

        awaitOrderPersisted(eventId);
    }

    @Test
    void shouldDocumentTheFailureModesItCanSimulate() throws Exception {
        mockMvc.perform(get("/orders/failure-modes"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['" + DemoFailureRules.ALWAYS_FAILING_ITEM + "']").isNotEmpty());
    }
}
