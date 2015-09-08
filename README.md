# EventTracker
接收事件数据并格式化成avro格式,然后打到kafka里.
## 用法
一般只需要指定配置文件就可以,启动参数的设置会覆盖配置文件里的设置.
```
    EventTracker
        -c <config.json>                                配置文件,一般来说所有参数都在配置文件里设定就足够了,下面的参数会覆盖配置文件的配置.
        -brokers <localhost:9022>                       broker list
        -topic <default,activation,order,registration>  kafka topics, 一共4个,英文逗号分割,按照所示顺序.
        -partitioner <hash | random | manual>           partitioner of kafka
        -partition <-1>                                 partition when manual(what's the meaning?)
        -schema <event.avsc>                            avro schema file.
        -port <1080>                                    http监听端口.
        -log <log.log>                                  logfile
        -backfile <backup.log>                          kafka写入失败时用来存储数据的暂存文件（没实现自动再写入kafka）.
```
## config.json
示例文件在`example_config`下。

注意: 若使用`front`,每一个应用实例会使用配置文件中的`front.backend_http_listen_address`作为监听地址,并忽略`main.port`,一般指定为一个内网ip+port的形式,并且向`front`注册.

注意: 端口为`0`代表随机使用一个可用端口.
## Tools : front
是一个前端反向代理,提供HA.
###用法
一般只需要指定好配置文件就可以.
```
    front
        -c <config.json>                                配置文件,一般来说所有参数都在配置文件里设定就足够了,下面的参数会覆盖配置文件的配置.
        -http_port <1080>                               最前端的http监听端口.
        -rpc_address <127.0.0.1:8081>                   服务注册端口,一般是一个LAN网ip.
        -log <log.log>                                  logfile.
        -F                                              强制使用,无视配置文件里面的enable字段.
        -B                                              (随机?)负载均衡?.我觉得有点不太随机.
```
先启动front,然后再启动EventTracker实例.
## 接口
### event接口
URL: `/event` method: `Post/Get`

必填参数: `did`(设备id),`timestamp`(unix utc second), `event_type`(默认三类:activation, registration, order.以及其他)

可选参数 `aid`(auction id),`ip`

### CSV接口
URL: `/`

CSV格式: 首行为各列标题，同event接口，其余为数据

###返回
成功会返回`HTTP 200`以及成功写入条数
