package cn.com.xx.reader.service;

import java.io.UnsupportedEncodingException;

public interface ReaderService {


    //目标系统starrocks/hana/mysql对应的表
    void read(String targetSchema, String tablename) throws  Exception;
}
