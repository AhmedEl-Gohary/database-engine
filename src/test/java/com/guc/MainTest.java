package com.guc;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class MainTest {

    @Test
    public void nameShouldBeAhmed() {
        String name = Main.getName();
        Assertions.assertEquals("ahmed", name);
    }
}
