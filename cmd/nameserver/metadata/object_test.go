package metadata

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPutObjectInfo(t *testing.T) {
	setup()
	var bi = BucketInfo{
		Bucketid:   "eb46ca9f21b248a6ab437710f2413333",
		Name:       "test_bucket_for_obj",
		Owner:      234,
		Policy:     "rwx",
		Versioning: VersioningEnabled,
	}
	//create test bucket
	err := PutBucketInfo(context.Background(), &bi)
	if err != nil {
		t.Error(err)
	}

	var oi = ObjectInfo{
		Name:           "obj2",
		Dirname:        "/dir1/dir2/dir3",
		Bucket:         "test_bucket_for_obj",
		Cid:            "testcid",
		Etag:           "testetag",
		Content_length: 1021,
		Content_type:   "text",
		Version:        "versionid1",
	}
	t.Run("put normal", func(t *testing.T) {
		err = PutObjectInfo(context.Background(), &oi)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("put existed object", func(t *testing.T) {
		err = PutObjectInfo(context.Background(), &oi)
		if err == nil {
			t.Log("test put an exist object ok")
		} else {
			t.Error("test put an exist object storageerror")
		}
	})
}

func TestQueryObjectInfo(t *testing.T) {
	setup()

	t.Run("normal", func(t *testing.T) {
		o, err := QueryObjectInfo(context.Background(), "test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "")
		if err != nil {
			t.Error(err)
		} else {
			m, _ := json.Marshal(o)
			t.Log("got object byte: ", string(m))
			retN := make(map[string]interface{})
			json.Unmarshal(m, &retN)
			t.Log("unmarshall ret:", retN)
		}
	})

	t.Run("not exist bucketid", func(t *testing.T) {
		_, err := QueryObjectInfo(context.Background(), "test_not_exist_bucketid", "/dir1/dir2/dir3", "obj1", "")
		if err == nil {
			t.Error("test not exist bucketid storageerror")
		} else {
			t.Log("test not exist bucketid ok")
		}
	})

	t.Run("emptry arg", func(t *testing.T) {
		_, err := QueryObjectInfo(context.Background(), "", "", "", "")
		if err == nil {
			t.Error("test wrong args storageerror")
		} else {
			t.Log("test wrong args ok")
		}
	})
}

//
//func TestQueryObjectInfosByPrefix(t *testing.T) {
//	setup()
//	t.Run("normal", func(t *testing.T) {
//		ois, err := QueryObjectInfosByPrefix(context.Background(), "test_bucket_for_obj", "/dir1/dir2/dir3", 0, 0)
//		if err != nil {
//			t.Errorf("got objects storageerror %s", err)
//		} else {
//			t.Log("got objects: ", ois)
//		}
//	})
//	t.Run("normal object", func(t *testing.T) {
//		ois, err := QueryObjectInfosByPrefix(context.Background(), "test_bucket_for_obj", "/dir1/dir2/dir3/obj2", 0, 0)
//		if err != nil {
//			t.Errorf("got objects storageerror %s", err)
//		} else {
//			t.Log("got objects: ", ois)
//		}
//	})
//
//	t.Run("abnormal", func(t *testing.T) {
//		ois, err := QueryObjectInfosByPrefix(context.Background(), "test_bucket_for_obj", "/bcd", 0, 0)
//		if len(ois) > 0 {
//			t.Error("abnormal test storageerror")
//		}
//		if err != nil {
//			t.Errorf("got objects storageerror %s", err)
//		} else {
//			t.Log("got objects: ", ois)
//		}
//	})
//}

func TestCheckObjectExistWithVersion(t *testing.T) {
	setup()
	var cases = []struct {
		Name     string
		opt      ObjectOptions
		expected bool
	}{
		{"normal", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, true},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			o, err := QueryObjectInfo(context.Background(), c.opt.Bucket, c.opt.Prefix, c.opt.Object, "")
			if err != nil {
				t.Error(err)
			} else {
				t.Log("got object: ", o)
			}
			ret := CheckObjectExist(context.Background(), c.opt)
			if c.expected != ret {
				t.Errorf("arg [%s,%s,%s,%s], return [%t], expect [%t]", c.opt.Bucket, c.opt.Prefix, c.opt.Object, c.opt.VersionID, ret, c.expected)
			}
		})
	}
}

func TestQueryObjectInfoAll(t *testing.T) {
	setup()
	ohis, err := QueryObjectInfoAll(context.Background(), "test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", "", 1024, false)
	if err != nil {
		t.Error(err)
	} else {
		t.Log(ohis)
	}
}

func TestPutObjectTags(t *testing.T) {
	setup()
	var cases = []struct {
		Name     string
		opt      ObjectOptions
		tags     string
		Expected error
	}{
		{"put", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"tags\":\"Anna\"}", nil},
		{"update", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"tags\":\"Anna,Linda\"}", nil},
		{"delete", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "", nil},
	}
	for _, c := range cases {
		oi, _ := QueryObjectInfo(context.Background(), c.opt.Bucket, c.opt.Prefix, c.opt.Object, "")
		c.opt.VersionID = oi.Version
		err := PutObjectTags(context.Background(), c.opt, c.tags)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetObjectTags(t *testing.T) {
	setup()
	var cases = []struct {
		Name     string
		opt      ObjectOptions
		tags     string
		Expected error
	}{
		{"put", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"tags\":\"Anna\"}", nil},
	}
	for _, c := range cases {
		oi, _ := QueryObjectInfo(context.Background(), c.opt.Bucket, c.opt.Prefix, c.opt.Object, "")
		c.opt.VersionID = oi.Version
		err := PutObjectTags(context.Background(), c.opt, c.tags)
		if err != c.Expected {
			t.Error(err)
		}
		c.opt.VersionID = oi.Version
		tags, err := QueryObjectTags(context.Background(), c.opt)
		if tags != c.tags {
			t.Error(err)
		} else {
			t.Log(tags)
		}
		err = PutObjectTags(context.Background(), c.opt, "")
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestPutObjectAcl(t *testing.T) {
	setup()
	var cases = []struct {
		Name     string
		opt      ObjectOptions
		Acl      string
		Expected error
	}{
		{"put", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"acl\":\"public-read\"}", nil},
		{"update", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"acl\":\"public-write\"}", nil},
		{"delete", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "", nil},
	}
	for _, c := range cases {
		oi, _ := QueryObjectInfo(context.Background(), c.opt.Bucket, c.opt.Prefix, c.opt.Object, "")
		c.opt.VersionID = oi.Version
		err := PutObjectAcl(context.Background(), c.opt, c.Acl)
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestGetObjectAcl(t *testing.T) {
	setup()
	var cases = []struct {
		Name     string
		opt      ObjectOptions
		Acl      string
		Expected error
	}{
		{"put", ObjectOptions{"test_bucket_for_obj", "/dir1/dir2/dir3", "obj2", "", true, true, false}, "{\"acl\":\"public-read\"}", nil},
	}
	for _, c := range cases {
		oi, _ := QueryObjectInfo(context.Background(), c.opt.Bucket, c.opt.Prefix, c.opt.Object, "")
		c.opt.VersionID = oi.Version
		err := PutObjectAcl(context.Background(), c.opt, c.Acl)
		if err != c.Expected {
			t.Error(err)
		}
		acl, err := QueryObjectAcl(context.Background(), c.opt)
		if acl != c.Acl {
			t.Error(err)
		}
		err = PutObjectAcl(context.Background(), c.opt, "")
		if err != c.Expected {
			t.Error(err)
		}
	}
}

func TestDeleteObjectInfo(t *testing.T) {
	setup()
	var oi = ObjectInfo{
		Name:           "obj2",
		Dirname:        "/abc",
		Bucket:         "test_bucket_for_obj",
		Cid:            "testcid",
		Etag:           "testetag",
		Content_length: 1021,
		Content_type:   "text",
		Version:        "versionid1",
	}

	err := PutObjectInfo(context.Background(), &oi)
	if err != nil {
		t.Error(err)
	}

	t.Run("normal", func(t *testing.T) {
		o, err := QueryObjectInfo(context.Background(), "test_bucket_for_obj", "/abc", "obj2", "")
		if err != nil {
			t.Error(err)
		} else {
			t.Log("got object: ", o)
		}
		_, err = DeleteObjectInfo(context.Background(), ObjectOptions{"test_bucket_for_obj", "/abc", "obj2", o.Version, true, true, false})
		if err != nil {
			t.Error(err)
		} else {
			t.Log("test normal delete ok")
		}
		_, err = DeleteObjectInfo(context.Background(), ObjectOptions{"test_bucket_for_obj", "/", "abc", Defaultversionid, true, true, true})
		if err != nil {
			t.Error(err)
		} else {
			t.Log("test normal delete ok")
		}
		_, err = DeleteObjectInfo(context.Background(), ObjectOptions{"test_bucket_for_obj", "/", "dir1", Defaultversionid, true, true, true})
		if err != nil {
			t.Error(err)
		} else {
			t.Log("test normal delete ok")
		}
	})

	// clear
	err = DeleteBucketInfo(context.Background(), "test_bucket_for_obj")
	if err != nil {
		t.Error(err)
	}
}
