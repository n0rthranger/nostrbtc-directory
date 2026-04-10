/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageQueueReturnValue {

    @JsonProperty("private_id")
    private int privateId;

    private GrapeRankResult result;

    public MessageQueueReturnValue(GrapeRankResult result, int privateId) {
        this.privateId = privateId;
        this.result = result;
    }

    public int getPrivateId() {
        return privateId;
    }

    public void setPrivateId(int privateId) {
        this.privateId = privateId;
    }

    public GrapeRankResult getResult() {
        return result;
    }

    public void setResult(GrapeRankResult result) {
        this.result = result;
    }
}
