package cn.doitedu.rtmk.engine.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Desc: 封装规则触达结果的javabean
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleMatchResult {

    private int guid;
    private String ruleId;
    private long matchTime;

}
