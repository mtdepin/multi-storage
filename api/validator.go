package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"mtcloud.com/mtstorage/pkg/logger"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"go4.org/sort"
)

type ParamsMap interface {
	GetQuery(key string) (string, bool)
}

type paramsSlice []paramKv

var KindOfIntList = reflect.TypeOf([]int64{}).Kind()

func (p paramsSlice) Len() int           { return len(p) }
func (p paramsSlice) Less(i, j int) bool { return p[i].k < p[j].k }
func (p paramsSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p paramsSlice) Slice() []string {
	tmp := make([]string, len(p))
	for index, v := range p {
		tmp[index] = v.String()
	}
	return tmp
}

type paramKv struct {
	k string
	v string
}

func (kv paramKv) String() string {
	return fmt.Sprintf("%s=%s", kv.k, kv.v)
}

type Validator struct {
	accessKey string
	secret    string
	timeout   time.Duration
	signCache *lru.Cache
}

func NewValidator(accessKey, secret string, timeout time.Duration) *Validator {
	c, _ := lru.New(300)
	return &Validator{
		accessKey: accessKey,
		secret:    secret,
		timeout:   timeout,
		signCache: c,
	}
}

// ReadBodyBytes reads the request body and returns bytes
func (val *Validator) ReadBodyBytes(req *http.Request) ([]byte, error) {
	if req.Body != nil {
		r := req
		buf, err := ioutil.ReadAll(r.Body)
		return buf, err
	}
	return nil, errors.New("Invalid Arguments")
}

func (val Validator) ReadJsonObject(req *http.Request, args interface{}) error {
	buf, err := val.ReadBodyBytes(req)
	logger.Debugf("request body : %s", buf)
	if err != nil {
		errMsg := fmt.Errorf("wrong request body : '%s'", err)
		logger.Error(errMsg)
		return errMsg
	}
	//err := json.NewDecoder(s.req.Body).Decode(&args)
	err = json.Unmarshal(buf, &args)
	if err != nil {
		errMsg := fmt.Errorf("wrong register input: '%s'", err)
		logger.Error(errMsg)
		return errMsg
	}
	return nil
}

func (val Validator) GetQuery(req *http.Request, key string) (string, bool) {
	params := req.URL.Query()
	if values, ok := params[key]; ok {
		return values[0], ok
	}
	return "", false
}

func (val Validator) Validate(pm *http.Request, paramsObj interface{}, method string, canReuse bool) error {
	vof := reflect.ValueOf(paramsObj)
	if vof.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("storageerror type of %s", vof.String()))
	}
	out := vof.Elem()
	tof := out.Type()
	numFiled := out.NumField()
	pl := make(paramsSlice, numFiled)
	for i := 0; i < numFiled; i++ {
		pname := tof.Field(i).Tag.Get("param")
		param, ok := val.GetQuery(pm, pname)
		if !ok {
			return fmt.Errorf("cannot find param: %s", pname)
		}
		switch out.Field(i).Kind() {
		case reflect.Int64:
			num, err := strconv.Atoi(param)
			if err != nil {
				return fmt.Errorf("convert int params storageerror: %s", param)
			}
			out.Field(i).SetInt(int64(num))
		case reflect.String:
			out.Field(i).SetString(param)
		case reflect.Bool:
			res, err := strconv.ParseBool(param)
			if err != nil {
				return fmt.Errorf("convert bool params storageerror: %s", param)
			}
			out.Field(i).SetBool(res)
		case KindOfIntList:
			ss := strings.Split(param, ",")
			l := make([]int64, 0)
			for _, s := range ss {
				num, err := strconv.Atoi(s)
				if err != nil {
					return fmt.Errorf("convert int params storageerror: %s", param)
				}
				l = append(l, int64(num))
			}
			out.Field(i).Set(reflect.ValueOf(l))
		default:
			panic("unsupported type params")
		}
		pl[i] = paramKv{
			k: pname,
			v: param,
		}
	}

	// time check
	err := val.TimeCheck(pm)
	if err != nil {
		return err
	}
	// sign check
	err = val.SignCheck(method, pm, pl, canReuse)
	if err != nil {
		return err
	}
	return nil
}

func (val Validator) BodyValidate(pm *http.Request, body []byte, method string) error {

	pl := make(paramsSlice, 1)
	pl[0] = paramKv{
		k: "body",
		v: base64.StdEncoding.EncodeToString(body),
	}

	// time check
	err := val.TimeCheck(pm)
	if err != nil {
		return err
	}
	// sign check
	err = val.SignCheck(method, pm, pl, false)
	if err != nil {
		return err
	}
	return nil
}

func (val Validator) SignCheck(method string, pm *http.Request, slice paramsSlice, canReuse bool) error {
	tstring, ok := val.GetQuery(pm, "timestamp")
	if !ok {
		return fmt.Errorf("cannot find param: timestamp")
	}
	sstring, ok := val.GetQuery(pm, "sign")
	if !ok {
		return fmt.Errorf("cannot find param: sign")
	}
	sstring = strings.TrimSpace(sstring)
	_, ok = val.signCache.Get(sstring)
	if ok {
		return fmt.Errorf("a used sign")
	}
	sort.Sort(slice)
	paramsString := strings.Join(slice.Slice(), "")
	enc := fmt.Sprintf("accesskey=%stimestamp=%smethod=%s%s%s", val.accessKey, tstring, method, paramsString, val.secret)
	fmt.Println("testckecout enc:", enc)
	sign := sha256.Sum256([]byte(enc))
	goSignString := hex.EncodeToString(sign[:])
	if sstring != goSignString {
		log.Errorf("sign storageerror enc: %s", enc)
		return fmt.Errorf("sign storageerror")
	}
	if !canReuse {
		val.signCache.Add(sstring, true)
	}
	return nil
}

func (val Validator) TimeCheck(pm *http.Request) error {
	tstring, ok := val.GetQuery(pm, "timestamp")
	if !ok {
		return fmt.Errorf("cannot find param: timestamp")
	}

	//timestamp check
	ts, err := strconv.Atoi(tstring)
	if err != nil {
		return fmt.Errorf("timestamp storageerror: %s", err.Error())
	}
	now := time.Now()
	paramTime := time.Unix(0, int64(ts)*int64(time.Millisecond))
	if now.After(paramTime) {
		return fmt.Errorf("request expired")
	}
	return nil
}

const secret = "KaSToOM10IsNooLfhnc8aSgD7TOPPgZcmMt2c/8zvwIDAQAB"
const accessId = "666888"

var v *Validator
var once sync.Once

func GetValidator() *Validator {
	once.Do(func() {
		v = NewValidator(accessId, secret, time.Hour)
	})
	return v
}
