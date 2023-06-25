package metadata

import (
	"context"
	"testing"
)

func TestPutBucketInfo(t *testing.T) {
	setup()
	var bi = BucketInfo{
		Bucketid:   "eb46ca9f21b248a6ab437710f241c078",
		Name:       "test_bucket",
		Owner:      123,
		Policy:     "rwx",
		Versioning: VersioningEnabled,
		Profile:    "Chengdu,Beijing,New York",
	}

	t.Run("normal", func(t *testing.T) {
		err := PutBucketInfo(context.Background(), &bi)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("commit existed bucket", func(t *testing.T) {
		err := PutBucketInfo(context.Background(), &bi)
		if err != nil {
			t.Log("put an existed bucket")
		}
	})
}

func TestCheckBucketExist(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		Expected     bool
	}{
		{"normal", "test_bucket", true},
		{"abnormal", "test_not_exist_bucket", false},
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			ret := CheckBucketExist(context.Background(), c.Bucket)
			if c.Expected != ret {
				t.Errorf("arg [%s], return [%t], expect [%t]", c.Bucket, ret, c.Expected)
			}
		})
	}
}

func TestQueryBucketInfo(t *testing.T) {
	setup()
	t.Run("normal", func(t *testing.T) {
		b, err := QueryBucketInfo(context.Background(), "test_bucket")
		if err != nil {
			t.Error(err)
		} else {
			t.Log("got bucket:", b)
		}
	})
	t.Run("query not exist bucket", func(t *testing.T) {
		_, err := QueryBucketInfo(context.Background(), "test_bucket_not_exist")
		if err == nil {
			t.Error("wrong return value", err)
		} else {
			t.Log("got bucket storageerror:", err)
		}
	})

	t.Run("query all buckets", func(t *testing.T) {
		bis, err := QueryAllBucketInfos(context.Background())
		if err != nil {
			t.Error(err)
		}
		if len(bis) == 0 {
			t.Error("get all buckets failed")
		}
	})
}

func TestQueryBucketInfoByOwner(t *testing.T) {
	setup()
	var bi = BucketInfo{
		Bucketid:   "eb46ca9f21b248a6ab437710f241c079",
		Name:       "test_bucket2",
		Owner:      123,
		Policy:     "rwx",
		Versioning: VersioningEnabled,
	}

	//new bucket for test
	err := PutBucketInfo(context.Background(), &bi)
	if err != nil {
		t.Error(err)
	}

	t.Run("normal", func(t *testing.T) {
		bis, err := QueryBucketInfoByOwner(context.Background(), bi.Owner)
		if err != nil {
			t.Error("query bucket infos storageerror")
		} else {
			t.Log("query bucket infos ok ", bis)
		}
	})

	t.Run("not exist user", func(t *testing.T) {
		bis, err := QueryBucketInfoByOwner(context.Background(), 456)
		if err != nil {
			t.Error("test empty user storageerror ", err)
		} else {
			t.Log("test empty user storageerror ok ", bis, err)
		}
	})

	//clear
	err = DeleteBucketInfo(context.Background(), "test_bucket2")
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateBucketInfo(t *testing.T) {
	setup()
	var bi = BucketInfo{
		Bucketid:   "eb46ca9f21b248a6ab437710f241c078",
		Name:       "test_bucket",
		Owner:      789,
		Policy:     "ro",
		Versioning: VersioningSuspended,
	}
	err := UpdateBucketInfo(context.Background(), &bi)
	if err != nil {
		t.Error(err)
	}
}

func TestGetStorageInfo(t *testing.T) {
	setup()
	s, err := GetStorageInfo(context.Background())
	if nil != err {
		t.Error(err)
	} else {
		t.Log(s)
	}
}

func TestPutBucketLogging(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		logging      string
		Expected     error
	}{
		{"put", "test_bucket", "{\"target\":\"testbucket\", \"prefix\":\"testprefix\"}", nil},
		{"update", "test_bucket", "{\"target\":\"testbucket\", \"prefix\":\"testprefix2\"}", nil},
		{"delete", "test_bucket", "", nil},
	}
	for _, c := range cases {
		err := PutBucketLogging(context.Background(), c.Bucket, c.logging)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestPutBucketPolicy(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		policy       string
		Expected     error
	}{
		{"put", "test_bucket", "{\"Version\": \"1\", \"Statement\":[{\"Action\":[\"oss:PutObject\",\"oss:GetObject\"],\"Effect\":\"Deny\",\"Principal\":[\"1234567890\"],\"Resource\":[\"acs:oss:*:1234567890:*/*\"]}]}", nil},
		{"update", "test_bucket", "{\"Version\": \"1\", \"Statement\":[{\"Action\":[\"oss:PutObject\",\"oss:GetObject\"],\"Effect\":\"Deny\",\"Principal\":[\"1234567890\"],\"Resource\":[\"acs:oss:*:1234567890:*/*\"]}]}", nil},
		{"delete", "test_bucket", "", nil},
	}
	for _, c := range cases {
		err := PutBucketPolicy(context.Background(), c.Bucket, c.policy)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetBucketPolicy(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		policy       string
		Expected     error
	}{
		{"put", "test_bucket", "{\"Version\": \"1\", \"Statement\":[{\"Action\":[\"oss:PutObject\",\"oss:GetObject\"],\"Effect\":\"Deny\",\"Principal\":[\"1234567890\"],\"Resource\":[\"acs:oss:*:1234567890:*/*\"]}]}", nil},
	}
	for _, c := range cases {
		err := PutBucketPolicy(context.Background(), c.Bucket, c.policy)
		if err != c.Expected {
			t.Error(err)
		}
		policy, err := QueryBucketPolicy(context.Background(), c.Bucket)
		if policy != c.policy {
			t.Error(err)
		}
		err = PutBucketPolicy(context.Background(), c.Bucket, "")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestPutBucketLifecycle(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		lifecycle    string
		Expected     error
	}{
		{"put", "test_bucket", "{\"lifecycletest\":\"v1\"}", nil},
		{"update", "test_bucket", "{\"lifecycletest\":\"v2\"}", nil},
		{"delete", "test_bucket", "", nil},
	}
	for _, c := range cases {
		err := PutBucketLifecycle(context.Background(), c.Bucket, c.lifecycle)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetBucketLifecycle(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		lifecycle    string
		Expected     error
	}{
		{"put", "test_bucket", "{\"lifecycletest\":\"v1\"}", nil},
	}
	for _, c := range cases {
		err := PutBucketLifecycle(context.Background(), c.Bucket, c.lifecycle)
		if err != c.Expected {
			t.Error(err)
		}
		lifecycle, err := QueryBucketLifecycle(context.Background(), c.Bucket)
		if lifecycle != c.lifecycle {
			t.Error(err)
		}
		err = PutBucketLifecycle(context.Background(), c.Bucket, "")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestPutBucketACL(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		acl          string
		Expected     error
	}{
		{"put", "test_bucket", "{\"acl\":\"public-read\"}", nil},
		{"update", "test_bucket", "{\"acl\":\"public-write\"}", nil},
		{"delete", "test_bucket", "", nil},
	}
	for _, c := range cases {
		err := PutBucketAcl(context.Background(), c.Bucket, c.acl)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetBucketACL(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		acl          string
		Expected     error
	}{
		{"put", "test_bucket", "{\"acl\":\"public-read\"}", nil},
	}
	for _, c := range cases {
		err := PutBucketAcl(context.Background(), c.Bucket, c.acl)
		if err != c.Expected {
			t.Error(err)
		}
		acl, err := QueryBucketAcl(context.Background(), c.Bucket)
		if acl != c.acl {
			t.Error(err)
		}
		err = PutBucketAcl(context.Background(), c.Bucket, "")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestPutBucketTags(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		tags         string
		Expected     error
	}{
		{"put", "test_bucket", "{\"tags\":\"Anna\"}", nil},
		{"update", "test_bucket", "{\"tags\":\"Anna,Linda\"}", nil},
		{"delete", "test_bucket", "", nil},
	}
	for _, c := range cases {
		err := PutBucketTags(context.Background(), c.Bucket, c.tags)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetBucketTags(t *testing.T) {
	setup()
	var cases = []struct {
		Name, Bucket string
		tags         string
		Expected     error
	}{
		{"put", "test_bucket", "{\"tags\":\"Anna\"}", nil},
	}
	for _, c := range cases {
		err := PutBucketTags(context.Background(), c.Bucket, c.tags)
		if err != c.Expected {
			t.Error(err)
		}
		tags, err := QueryBucketTags(context.Background(), c.Bucket)
		if tags != c.tags {
			t.Error(err)
		}
		err = PutBucketTags(context.Background(), c.Bucket, "")
		if err != nil {
			t.Error(err)
		}
	}
}
func TestDeleteBucketInfo(t *testing.T) {
	setup()
	t.Run("normal", func(t *testing.T) {
		err := DeleteBucketInfo(context.Background(), "test_bucket")
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("not exist bucket", func(t *testing.T) {
		err := DeleteBucketInfo(context.Background(), "test_bucket_not_exist")
		if err == nil {
			t.Error("test delete not exist bucket storageerror")
		} else {
			t.Log("test delete not exist bucket ok")
		}
	})
	t.Run("emptry arg", func(t *testing.T) {
		err := DeleteBucketInfo(context.Background(), "")
		if err == nil {
			t.Error("test empty arg storageerror")
		} else {
			t.Log("test empty arg ok")
		}
	})
}
