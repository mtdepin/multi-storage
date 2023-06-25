package httpapi

import (
	"net/http"
	"net/url"
	"testing"
)

func Test_parseObjectOptions(t *testing.T) {
	ur := "http://192.168.1.194:8522/ns/v1/object/acl?bucket=testabcd&object=%2Frsync-3.2.3+20200903+git9f9240b-4.tar&versionId="
	//l := url.QueryEscape(ur)

	parse, err := url.Parse(ur)
	q := parse.Query()
	t.Log(q)
	parse.RawQuery = url.QueryEscape(parse.RawQuery)
	t.Log(parse.String())
	r := &http.Request{
		URL: parse,
	}
	if err != nil {
		return
	}
	t.Log(r.FormValue("object"))
	t.Log(r.URL.Query().Get("object"))
}
