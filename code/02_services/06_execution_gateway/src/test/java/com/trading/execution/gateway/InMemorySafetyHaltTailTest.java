package com.trading.execution.gateway;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class InMemorySafetyHaltTailTest {
    GateRow boot(String pid, String acct){ return new GateRow(pid,acct,GateState.HALTED,0,"boot","ev0",null,null,null,null,0L,null,null,null); }

    SafetyHaltRequest req(String acct,String pid,String reason,long ts){
        String id=SafetyHaltRequest.deterministicId(acct,pid,"signal-job",reason,ts,1L,"abc123","v3");
        return new SafetyHaltRequest(id,acct,pid,"signal-job","i-1",reason,"detail",ts,1L,"abc123","v3",null,0L,null,null);
    }

    @Test void deterministicIdStable(){
        String a=SafetyHaltRequest.deterministicId("acct-1","p-1","signal-job","RISK",1000L,1L,"h1","v3");
        String b=SafetyHaltRequest.deterministicId("acct-1","p-1","signal-job","RISK",1000L,1L,"h1","v3");
        assertEquals(a,b); assertEquals(64,a.length());
        String c=SafetyHaltRequest.deterministicId("acct-1","p-1","signal-job","RISK",1001L,1L,"h1","v3");
        assertNotEquals(a,c);
    }

    @Test void deterministicIdRejectsDelimiterAmbiguity(){
        // P3-387: the canonical tuple is joined on '|' — an embedded delimiter lets
        // two different tuples collide on one PK, so an id is never minted for it.
        assertThrows(IllegalArgumentException.class, () ->
                SafetyHaltRequest.deterministicId("a|b","c","signal-job","RISK",1000L,1L,"h1","v3"));
        assertThrows(IllegalArgumentException.class, () ->
                SafetyHaltRequest.deterministicId("acct-1","b|c","signal-job","RISK",1000L,1L,"h1","v3"));
        assertThrows(IllegalArgumentException.class, () ->
                SafetyHaltRequest.deterministicId("acct-1","p-1","signal|job","RISK",1000L,1L,"h1","v3"));
        // idValid() is a predicate over decoded rows: an ambiguous form is reported
        // invalid — it must not escape as an exception.
        String id=SafetyHaltRequest.deterministicId("acct-1","p-1","signal-job","RISK",1000L,1L,"h1","v3");
        SafetyHaltRequest ambiguous=new SafetyHaltRequest(id,"a|b","p-1","signal-job","i-1","RISK","detail",1000L,1L,"h1","v3",null,0L,null,null);
        assertFalse(ambiguous.idValid());
    }

    @Test void idempotentDuplicateNoSecondEpoch(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        long epoch0=gates.read("p-1").epoch();
        InMemoryControlStateStore store=new InMemoryControlStateStore();
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        SafetyHaltRequest r=req("acct-1","p-1","MANUAL_HALT",2000L);
        store.add(r);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(r,3000L));
        long epoch1=gates.read("p-1").epoch();
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(r,4000L));
        assertEquals(epoch1, gates.read("p-1").epoch());
        tail.replay(store,5000L);
        assertEquals(epoch1, gates.read("p-1").epoch());
    }

    @Test void crossScopeRejectDoesNotHalt(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        InMemoryControlStateStore store=new InMemoryControlStateStore();
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        SafetyHaltRequest r=req("acct-OTHER","p-1","MANUAL_HALT",2000L);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.CROSS_SCOPE_REJECT, tail.apply(r,3000L));
        assertEquals(GateState.HALTED, gates.read("p-1").state());
        SafetyHaltRequest ok=req("acct-1","p-1","MANUAL_HALT",2000L);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(ok,4000L));
    }
    @Test void invalidIdRejected(){
        SafetyHaltRequest bad=new SafetyHaltRequest("bad-id","acct-1","p-1","signal-job","i-1","MANUAL_HALT","detail",2000L,1L,"abc123","v3",null,0L,null,null);
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.INVALID_ID, tail.apply(bad,3000L));
    }
}
