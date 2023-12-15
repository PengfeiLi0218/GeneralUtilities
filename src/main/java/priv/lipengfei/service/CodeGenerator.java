package priv.lipengfei.service;

import lombok.*;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class CodeGenerator {
    private int length;
    private List<Rule> rules;
    private Map<String, Map<String, String>> mappers;

}
