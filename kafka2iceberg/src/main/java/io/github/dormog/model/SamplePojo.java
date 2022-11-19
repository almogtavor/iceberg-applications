package io.github.dormog.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class SamplePojo {
    private String itemId;
    private String name;
    private String coolId;
    private Integer age;
    private Date createdDate;
}
