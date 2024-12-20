package cn.com.xx.reader.entity;

import cn.com.xx.common.base.BaseObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;

/**
 * 数据同步记录表
 */
@Data
@AllArgsConstructor
@Table(name="data_sync_records")
public class DataSyncRecords extends BaseObject {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    public Long id;//
    @Column(name = "targetschema")
    private String targetschema;//目标库
    @Column(name = "tablename")
    private String tablename;//
    @Column(name = "lastmodifieddate")
    private Date lastmodifieddate;//最后修改时间,对应mac系统中该表最后修改时间
    @Column(name = "createtime")
    private Date createtime;//日志表同步时间



    public DataSyncRecords(String targetSchema, String tablename, Date lastmodifieddate, Date createtime) {
        super();
        this.targetschema = targetSchema;
        this.tablename = tablename;
        this.lastmodifieddate = lastmodifieddate;
        this.createtime = createtime;
    }
}
