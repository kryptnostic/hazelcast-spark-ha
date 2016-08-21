package com.kryptnostic.sparks;

import org.junit.Test;

public class EclipseTriggerBug {

    @Test
    public void triggerSerializationBug() {
        EclipseBugRepro.main( new String[] {} );
    }
}
