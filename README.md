# EventTracker
接收事件数据并格式化成avro格式，然后打到kafka里。
## 用法
一般只需要指定配置文件就可以，其余参数的设置会覆盖配置文件里的设置。
```
    EventTracker
        -c <config.json>    path to your config file, this is the only param a standard user will use, more flags will override the config.
        -brokers <localhost:9022>  broker list
        -topic <default,activation,order,registration>  topics you want to use
        -partitioner <hash | random | manual>   partitioner of kafka
        -partition <-1> partition when manual(what's the meaning?)
        -schema <event.avsc>    avro schema file
        -port <1080>    http api service listening port
        -log <log.log>  logfile
```
## config.json
示例文件在`example_config`下

