package cn.com.xx.reader.entity;

import cn.com.xx.common.base.BaseObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import javax.persistence.*;

/**
 * 源库业务表配置
 */
@Data
@AllArgsConstructor
@Table(name="mac_table_config")
public class MacTableConfig extends BaseObject {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    public Long id;//
    @Column(name = "targetschema")
    private String targetschema;//目标库
    @Column(name = "tablename")
    private String tablename;//源库表名
    @Column(name = "targettablename")
    private String targettablename;//目标库表名
    @Column(name = "querysql")
    private String querysql;//


}
