package com.client.elatic;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.annotate.JsonIgnore;

@NoArgsConstructor
@Data
public class Employee {

    private String name;
    private int age;
    private int experienceInYears;

}
