# data-sync
Cross-platform data migration  跨平台数据迁移

本项目有common、reader、writer三个模块组成，实现跨平台数据的迁移。reader单独进程负责读取源数据（本实例读取的是salesforce），common公共配置模块，writer写入数据模块。

技术:dolphinscheduler+java+springmvc + springboot + kafka + mysql + starrocks

reader : 
   1.提供restapi接口 供dolphinscheduler调度

   2.对需要迁移的表，配置源库表配置在数据库配置表里，用于读取源数据

   3.数据读取完成把数据写入kafka


writer ：

   1.启动多线程消费kafka消息，消息体中包含源表字段属性及多条数据

   2.解析数据后批量写入目的库



如有新增表只需在reader模块配置表里，写入相关表信息
   


