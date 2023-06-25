package logarchive

import "encoding/xml"

/*
日志文件命名规则: <TargetPrefix><SourceBucket>YYYY-mm-DD-HH-MM-SS-UniqueString

TargetPrefix	日志文件的文件名前缀
SourceBucket	产生访问日志的源Bucket名称
YYYY-mm-DD-HH-MM-SS	日志文件被创建的时间。从左到右分别表示：年、月、日、小时、分钟和秒
UniqueString	系统生成的字符串，是日志文件的唯一标识
*/

// 日志配置信息格式
/*
<?xml version="1.0" encoding="UTF-8"?>
<BucketLoggingStatus>
    <LoggingEnabled>
        <TargetBucket>TargetBucket</TargetBucket>
        <TargetPrefix>TargetPrefix</TargetPrefix>
    </LoggingEnabled>
</BucketLoggingStatus>
*/

type BucketLoggingRet struct {
	XMLName xml.Name              `xml:"BucketLoggingStatus"`
	Enabled *BucketLoggingEnabled `xml:"LoggingEnabled"`
}

type BucketLoggingEnabled struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix"`
}
