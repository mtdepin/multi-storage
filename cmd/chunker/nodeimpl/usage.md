chunker 调用 nameserver  的方法
    
    //通过node的NameServer实例调用 nameserver方法
    n.NameServer.Heartbeat(ctx, n.getHeartbeatInfo())

    n.NameServer.Version(client.WithTrack(nil)


    调用时使用 client.WithTrack(nil) 表示实时等待返回，否则为不等待chunker方法返回

#强烈建议#
调用时使用 client.WithKey(ctx, "thisisatestkey") 传入业务码，方便业务跟踪和二次消费

没有业务码时会默认生成一个业务key

