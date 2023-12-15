package priv.lipengfei.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Rule {
    private int start;
    private int end;
    private String mapper;
}
