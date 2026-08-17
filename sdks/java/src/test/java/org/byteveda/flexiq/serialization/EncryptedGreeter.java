package org.byteveda.flexiq.serialization;

import org.byteveda.flexiq.annotation.Encrypted;
import org.byteveda.flexiq.annotation.TaskHandler;

/** Test fixture: an @Encrypted handler; the generated task carries the "encrypted" codec. */
class EncryptedGreeter {

    @TaskHandler("eg.greet")
    @Encrypted
    String greet(String name) {
        return "secret " + name;
    }
}
