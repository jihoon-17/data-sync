package cn.com.xx.reader.mapper;


import cn.com.xx.reader.entity.MacTableConfig;
import tk.mybatis.mapper.common.Mapper;

public interface MacTableConfigMapper extends Mapper<MacTableConfig> {

    MacTableConfig findQuerySqlBySchemaAndTable(String targetschema, String tablename);


}
