package cn.com.xx.reader.mapper;

import cn.com.xx.reader.entity.DataSyncRecords;
import tk.mybatis.mapper.common.Mapper;

public interface DataSyncRecordsMapper extends Mapper<DataSyncRecords> {

    DataSyncRecords findOneBySchemaAndTable(String targetschema, String tablename);

    void insertRecord(DataSyncRecords dataSyncRecord);

}
