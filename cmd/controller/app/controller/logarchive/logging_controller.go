package logarchive

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/robfig/cron/v3"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/logger"

	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder"
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/runtime"
)

type Entry struct {
	Timestamp          time.Time `json:"@timestamp"`
	RemoteIP           string    `json:"remoteIP"`           // 请求者的IP地址
	Reserved1          string    `json:"-"`                  // 保留字段，固定值为-
	Reserved2          string    `json:"-"`                  // 保留字段，固定值为-
	Time               string    `json:"time"`               // 收到请求的时间
	RequestURL         string    `json:"requestURL"`         // 请求的URL
	HTTPStatus         int       `json:"httpStatus"`         // 返回的HTTP状态码
	SentBytes          string    `json:"sentBytes"`          // 请求产生的下行流量
	RequestTime        string    `json:"requestTime"`        // 请求耗费的时间，单位：ms
	Referer            string    `json:"referer"`            // 请求的HTTP Referer
	UserAgent          string    `json:"userAgent"`          // HTTP的User-Agent头
	HostName           string    `json:"hostName"`           // 请求访问的目标域名
	RequestID          string    `json:"requestID"`          // 请求的Request ID
	LoggingFlag        bool      `json:"loggingFlag"`        // 是否已开启日志转存
	RequesterID        string    `json:"requesterID"`        // 请求者的用户ID，取值-表示匿名访问
	Operation          string    `json:"operation"`          // 请求类型
	BucketName         string    `json:"bucketName"`         // 请求的目标Bucket名称
	ObjectName         string    `json:"objectName"`         // 请求的目标Object名称
	ObjectSize         string    `json:"objectSize"`         // 目标Object大小
	ServerCostTime     string    `json:"serverCostTime"`     // 本次请求所花的时间，单位：毫秒
	ErrorCode          string    `json:"errorCode"`          // 返回的错误码，取值-表示未返回错误码
	RequestLength      int64     `json:"requestLength"`      // 请求的长度
	UserID             string    `json:"userID"`             // Bucket拥有者ID
	DeltaDataSize      string    `json:"deltaDataSize"`      // Object大小的变化量，取值-表示此次请求不涉及Object的写入操作
	SyncRequest        string    `json:"syncRequest"`        // 请求是否为CDN回源请求
	StorageClass       string    `json:"storageClass"`       // 目标Object的存储类型
	TargetStorageClass string    `json:"targetStorageClass"` // 是否通过生命周期规则或CopyObject转换了Object的存储类型
	AccessPoint        string    `json:"accessPoint"`        // 通过传输加速域名访问目标Bucket时使用的传输加速接入点
	AccessKeyID        string    `json:"accessKeyID"`        // 请求者的AccessKey ID，取值-表示匿名请求
	Version            string    `json:"version"`
	DeploymentID       string    `json:"deploymentID"`
}

// Controller manages selector-based service bucket.
type Controller struct {
	nameserverClient *clientbuilder.NameserverClient
	elastic          *elasticConfig
	elasticCli       *elastic.Client
}

// NewLoggingController returns a new *Controller.
func NewLoggingController(nscli *clientbuilder.NameserverClient) *Controller {

	c := &Controller{
		nameserverClient: nscli,
	}

	c.elastic = c.initElasticConfig()
	c.elasticCli, _ = ConnectES([]string{c.elastic.endpoint}, c.elastic.username, c.elastic.password)
	return c
}

// 初始化elasticsearch配置
func (c *Controller) initElasticConfig() *elasticConfig {
	return &elasticConfig{
		endpoint: config.GetString("elasticsearch.endpoint"),
		username: config.GetString("elasticsearch.username"),
		password: config.GetString("elasticsearch.password"),
	}
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {

	go func() {
		defer runtime.HandleCrash()
	}()

	go c.worker(stopCh)
	loggingConfigMap := make(map[string]BucketLoggingRet)

	for bucketName, loggingConfig := range loggingConfigMap {
		go c.bucketLogArchive(bucketName, loggingConfig)
	}

	<-stopCh
}

func (c *Controller) worker(stopCh <-chan struct{}) {
	cron := cron.New(cron.WithSeconds())
	spec := "0 0 * * * *"

	cron.AddFunc(spec, func() {
		//get random chunker node
		// GetBucketLogging() ([]BucketLoggingRet) 获取所有开启日志转存的bucket配置

		for bucketName, loggingConfig := range c.getBucketsLoggingConf() {
			c.bucketLogArchive(bucketName, loggingConfig)
		}
	})

	cron.Start()

	select {
	case <-stopCh:
		cron.Stop()
	}
}

func (c *Controller) getBucketsLoggingConf() map[string]BucketLoggingRet {
	result, err := c.nameserverClient.GetBucketsLogging(client.WithTrack(nil))
	if err != nil && result == nil {
		return nil
	}
	loggingConfigMap := make(map[string]BucketLoggingRet)
	for i := range result {
		log := make(map[string]string)
		json.Unmarshal([]byte(result[i].Log), &log)
		loggingConfigMap[result[i].Name] = BucketLoggingRet{
			Enabled: &BucketLoggingEnabled{
				TargetBucket: log["target"],
				TargetPrefix: log["prefix"],
			},
		}
	}
	return loggingConfigMap
}

func (c *Controller) bucketLogArchive(bucketName string, loggingConfig BucketLoggingRet) {
	logger.Infof("%s logging archive start--- loggingConfig TargetBucket: %s, TargetPrefix: %s",
		bucketName, loggingConfig.Enabled.TargetBucket, loggingConfig.Enabled.TargetPrefix)
	now := time.Now()
	EsResultCh := make(chan *elastic.SearchResult, 1)
	go c.getLogForEs(bucketName, now, EsResultCh)

	// <TargetPrefix><SourceBucket>YYYY-mm-DD-HH-MM-SS-UniqueString
	file, err := c.writeLog(loggingConfig, now, EsResultCh)
	defer os.Remove(file.Name())
	defer file.Close()
	if err != nil {
		return
	}
	defer os.Remove(file.Name())
	defer file.Close()
	// 将file指向文件开始的位置
	_, err = file.Seek(0, 0)
	if err != nil {
		logger.Info("logger=====>err:", err)
		return
	}
	// PutObject 推送日志文件

	c.uploadLogFile(file, loggingConfig)

}

func (c *Controller) getLogForEs(bucketName string, now time.Time, EsResultCh chan<- *elastic.SearchResult) {
	defer close(EsResultCh)
	rangeQuery := elastic.NewRangeQuery("@timestamp").Gte(now.Add(-time.Hour)).Lt(now)
	logger.Info("logger====>时间区间", now.Format("2006-01-02 15:04:05"))
	Scroll, err := c.elasticCli.Scroll().Index("bucket_name_" + bucketName).Query(rangeQuery).Size(5000).Scroll("1m").Do(context.TODO())
	count := 0
	if err != nil || Scroll.TotalHits() <= 0 {
		logger.Errorf("es 获取日志总数失败或日志条数为0，bucketName:", bucketName)
		return
	}
	if Scroll.TotalHits() < 5000 {
		count += len(Scroll.Hits.Hits)
		EsResultCh <- Scroll
		return
	}
	EsResultCh <- Scroll
	for {
		Scroll, err = c.elasticCli.Scroll().Scroll("1m").ScrollId(Scroll.ScrollId).Do(context.TODO())
		count += len(Scroll.Hits.Hits)
		if err != nil && err == io.EOF {
			break
		}
		if Scroll.TotalHits() < 5000 {
			EsResultCh <- Scroll
			break
		}
		EsResultCh <- Scroll
	}
	defer c.elasticCli.Scroll().ScrollId(Scroll.ScrollId).Clear(context.TODO())
}

func (c *Controller) writeLog(loggingConfig BucketLoggingRet, now time.Time, EsResultCh <-chan *elastic.SearchResult) (*os.File, error) {
	query := strings.Builder{}
	query.WriteString(loggingConfig.Enabled.TargetBucket)
	query.WriteString(now.Format("2006-01-02-15-04-05"))
	query.WriteString("-0001")
	fileName := query.String()
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Error("open log file err: ", err)
		return nil, err
	}
	fileBuffer := bufio.NewWriter(file)

	query.Reset()
	entry := Entry{}
	for result := range EsResultCh {
		if len(result.Hits.Hits) <= 0 {
			return file, nil
		}
		for _, item := range result.Each(reflect.TypeOf(entry)) {
			query.WriteString(fmt.Sprintf("%s\n", loggingField(item.(Entry))))
		}
		fileBuffer.WriteString(query.String())
		fileBuffer.Flush()
	}
	return file, nil
}

func (c *Controller) uploadLogFile(file *os.File, loggingConfig BucketLoggingRet) {
	node, err := c.nameserverClient.GetChunkerNode(client.WithTrack(nil))
	if err != nil {
		logger.Error("get chunkerNode err: ", err)
		return
	}
	putUrl := fmt.Sprintf("http://%s/cs/v1/object", node.Endpoint)
	bodys := &bytes.Buffer{}
	objectname := file.Name()
	if loggingConfig.Enabled.TargetPrefix != "" {
		if strings.HasSuffix(loggingConfig.Enabled.TargetPrefix, "/") {
			objectname = loggingConfig.Enabled.TargetPrefix + file.Name()
		} else {
			objectname = loggingConfig.Enabled.TargetPrefix + "/" + file.Name()
		}
	}
	writer := multipart.NewWriter(bodys)
	writer.WriteField("bucket", loggingConfig.Enabled.TargetBucket)
	writer.WriteField("objectname", objectname)
	writer.WriteField("content-type", "multipart/form-data")
	writer.WriteField("storageclass", "STANDARD")
	part, err := writer.CreateFormFile("object", objectname)
	if err != nil {
		logger.Error("open log file err: ", err)
		return
	}
	io.Copy(part, file)
	file.Seek(0, 0)
	all, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Info(err.Error())
		return
	}
	writer.WriteField("md5sum", fmt.Sprintf("%x", md5.Sum(all)))
	writer.Close()
	request, err := http.NewRequest(http.MethodPost, putUrl, bodys)
	if err != nil {
		logger.Error("make request err: ", err)
		return
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())

	do, err := http.DefaultClient.Do(request)
	if err != nil {
		logger.Error("upload file err: ", err)
		return
	}
	defer do.Body.Close()

}

func loggingField(source Entry) string {
	query := strings.Builder{}
	query.WriteString(source.RemoteIP)
	query.WriteString(" - - ") // 保留字段
	query.WriteString("[")
	logTime, _ := time.ParseInLocation(time.RFC3339Nano, source.Time, time.Local)
	cstSh, _ := time.LoadLocation("Asia/Shanghai")
	query.WriteString(logTime.In(cstSh).Format("02/Jan/2006 15:04:05 -0700"))
	query.WriteString("] ")
	query.WriteString(`"`)
	query.WriteString(source.RequestURL)
	query.WriteString(`" `)
	query.WriteString(fmt.Sprintf("%d", source.HTTPStatus))
	query.WriteString(" ")
	query.WriteString(source.SentBytes)
	query.WriteString(" ")
	query.WriteString(source.RequestTime)
	query.WriteString(` "`)
	query.WriteString(source.Referer)
	query.WriteString(`" "`)
	query.WriteString(source.UserAgent)
	query.WriteString(`" "`)
	query.WriteString(source.HostName)
	query.WriteString(`" "`)
	query.WriteString(source.RequestID)
	query.WriteString(`" "`)
	query.WriteString(strconv.FormatBool(source.LoggingFlag))
	query.WriteString(`" "`)
	query.WriteString(source.RequesterID)
	query.WriteString(`" "`)
	query.WriteString(source.Operation)
	query.WriteString(`" "`)
	query.WriteString(source.BucketName)
	query.WriteString(`" "`)
	query.WriteString(source.ObjectName)
	query.WriteString(`" "`)
	query.WriteString(source.ObjectSize)
	query.WriteString(`" "`)
	query.WriteString(source.ServerCostTime)
	query.WriteString(`" "`)
	query.WriteString(source.ErrorCode)
	query.WriteString(`" "`)
	query.WriteString(fmt.Sprintf("%d", source.RequestLength))
	query.WriteString(`" "`)
	query.WriteString(source.UserID)
	query.WriteString(`" "`)
	query.WriteString(source.DeltaDataSize)
	query.WriteString(`" "`)
	query.WriteString(source.SyncRequest)
	query.WriteString(`" "`)
	query.WriteString(source.StorageClass)
	query.WriteString(`" "`)
	query.WriteString(source.TargetStorageClass)
	query.WriteString(`" "`)
	query.WriteString(source.AccessPoint)
	query.WriteString(`" "`)
	query.WriteString(source.AccessKeyID)
	query.WriteString(`"`)
	return query.String()
}
