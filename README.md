# legend_db

## 介绍

此数据库是采用`Rust`编码实现，内存存储采用`Rust`自带的`Btreemap`数据实现

持久化采用较为简单的`Bitcask`模型，后续添加`B+`树，`LSM`树，并添加持久化模型配置

采用`thiserror`处理相关错误

采用`bincode`序列化和反序列化