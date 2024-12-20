package cn.com.xx.reader.controller;

import cn.com.xx.reader.enums.SchemaType;
import cn.com.xx.reader.service.ReaderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据读取入口
 * @author jihoon
 */
@Slf4j
@RestController
@RequestMapping("/data")
public class DataReadController {

    @Qualifier("read4RomaDBService")
    @Resource
    ReaderService read4RomaDBService;

    @Qualifier("read4HanaBIService")
    @Resource
    ReaderService read4HanaBIService;

    @Qualifier("read4MysqlDBService")
    @Resource
    ReaderService read4MysqlDBService;

    /**
     * 数据读取
     */
    @GetMapping("/reader")
    public void dataread(@RequestParam("targetSchema")String targetSchema, @RequestParam("tablename")String tablename){


        try{
            //根据targetSchema匹配相应的实现类
            ReaderService readerService = getReaderServiceMap().get(targetSchema);
            readerService.read(targetSchema, tablename);

        }catch (Exception e){

            log.error("dataread error:", e);

        }

    }

    /**
     * 实现类
     * @return
     */
    private Map<String, ReaderService> getReaderServiceMap(){

        Map<String, ReaderService> map = new HashMap<>();

        map.put(SchemaType.STARROCKS.getValue(), read4RomaDBService);
        map.put(SchemaType.MYSQL.getValue(), read4MysqlDBService);
        map.put(SchemaType.HANA.getValue(), read4HanaBIService);
        return map;

    }

}
