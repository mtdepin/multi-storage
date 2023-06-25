

nameserver 调用 chunker方法

    //通过chunkerId 创建chunker client实例
    chunker, err := client.CreateChunkerClient(context.Background(), chunkerId)
    if err != nil {
    logger.Error(err)
    continue
    }
    //使用chunker client实例 调用chunker方法
    info, err := chunker.Health(client.WithTrack(nil))
    if err != nil {
    logger.Error(err)
    continue
    }


    调用时使用 client.WithTrack(nil) 表示实时等待返回，否则为不等待chunker方法返回
