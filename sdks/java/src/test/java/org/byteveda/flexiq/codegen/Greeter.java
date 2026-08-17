package org.byteveda.flexiq.codegen;

import java.util.List;
import org.byteveda.flexiq.annotation.TaskHandler;

/** Test fixture: the {@code @TaskHandler} processor turns this into a generated {@code GreeterTasks}. */
class Greeter {

    @TaskHandler("greet")
    String greet(String name) {
        return "hello " + name;
    }

    @TaskHandler
    Integer total(List<Integer> numbers) {
        return numbers.stream().mapToInt(number -> number.intValue()).sum();
    }
}
