package cn.com.xx.common.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.json.JSONArray;

/**
 * 生产者消息载体
 */
@Data
@AllArgsConstructor
public class DataVo {

    private String msg;//表数据

    private String columns;//列名

    private String targettablename;

}
