package logarchive

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"mtcloud.com/mtstorage/pkg/logger"

	"github.com/olivere/elastic/v7"
	"github.com/tidwall/gjson"
	xhttp "mtcloud.com/mtstorage/pkg/http"
)

type elasticConfig struct {
	endpoint string
	username string
	password string
}

const elasticTimeout = 5 * time.Second

func ConnectES(address []string, userName, password string) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetURL(address...),
		elastic.SetBasicAuth(userName, password),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
		elastic.SetSniff(false),
	)

	if err != nil {
		return nil, err
	}
	return client, nil
}

func (e *elasticConfig) getElasticStatus() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	endpoint := e.endpoint + "/_cat/health"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		logger.Error("elasticsearch abnormal status ", err)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(e.username, e.password)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("elasticsearch abnormal status ", err)
		return false
	}

	xhttp.DrainBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		logger.Errorf("status: %s, return status error", resp.Status)
		return false
	}

	return true
}

func (e *elasticConfig) getElasticLogging(bucketName string, body io.Reader) (*gjson.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), elasticTimeout)
	defer cancel()

	endpointBuilder := strings.Builder{}
	endpointBuilder.WriteString(e.endpoint)
	if bucketName != "" {
		endpointBuilder.WriteString("/")
		endpointBuilder.WriteString("bucket_name_" + bucketName)
		endpointBuilder.WriteString("/_doc")
	}
	endpointBuilder.WriteString("/_search")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointBuilder.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(e.username, e.password)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status: %s, return status error", resp.Status)
	}

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	xhttp.DrainBody(resp.Body)

	result := gjson.Get(string(respData), "hits")

	return &result, nil
}

func (e *elasticConfig) getElasticLoggingCount(bucketName string, body io.Reader) (int64, error) {
	result, err := e.getElasticLogging(bucketName, body)
	if err != nil {
		return 0, err
	}
	return result.Get("total.value").Int(), nil
}
