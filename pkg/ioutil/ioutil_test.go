package ioutil

import (
	"bytes"
	"context"
	"io"
	goioutil "io/ioutil"
	"os"
	"testing"
	"time"
)

type sleepWriter struct {
	timeout time.Duration
}

func (w *sleepWriter) Write(p []byte) (n int, err error) {
	time.Sleep(w.timeout)
	return len(p), nil
}

func (w *sleepWriter) Close() error {
	return nil
}

func TestDeadlineWriter(t *testing.T) {
	w := NewDeadlineWriter(&sleepWriter{timeout: 500 * time.Millisecond}, 450*time.Millisecond)
	_, err := w.Write([]byte("1"))
	w.Close()
	if err != context.Canceled {
		t.Error("DeadlineWriter shouldn't be successful - should return context.Canceled")
	}
	_, err = w.Write([]byte("1"))
	if err != context.Canceled {
		t.Error("DeadlineWriter shouldn't be successful - should return context.Canceled")
	}
	w = NewDeadlineWriter(&sleepWriter{timeout: 100 * time.Millisecond}, 600*time.Millisecond)
	n, err := w.Write([]byte("abcd"))
	w.Close()
	if err != nil {
		t.Errorf("DeadlineWriter should succeed but failed with %s", err)
	}
	if n != 4 {
		t.Errorf("DeadlineWriter should succeed but should have only written 4 bytes, but returned %d instead", n)
	}
}

func TestCloseOnWriter(t *testing.T) {
	writer := WriteOnClose(goioutil.Discard)
	if writer.HasWritten() {
		t.Error("WriteOnCloser must not be marked as HasWritten")
	}
	writer.Write(nil)
	if !writer.HasWritten() {
		t.Error("WriteOnCloser must be marked as HasWritten")
	}

	writer = WriteOnClose(goioutil.Discard)
	writer.Close()
	if !writer.HasWritten() {
		t.Error("WriteOnCloser must be marked as HasWritten")
	}
}

// Test for AppendFile.
func TestAppendFile(t *testing.T) {
	f, err := goioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name1 := f.Name()
	defer os.Remove(name1)
	f.WriteString("aaaaaaaaaa")
	f.Close()

	f, err = goioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name2 := f.Name()
	defer os.Remove(name2)
	f.WriteString("bbbbbbbbbb")
	f.Close()

	if err = AppendFile(name1, name2, false); err != nil {
		t.Error(err)
	}

	b, err := goioutil.ReadFile(name1)
	if err != nil {
		t.Error(err)
	}

	expected := "aaaaaaaaaabbbbbbbbbb"
	if string(b) != expected {
		t.Errorf("AppendFile() failed, expected: %s, got %s", expected, string(b))
	}
}

func TestSkipReader(t *testing.T) {
	testCases := []struct {
		src      io.Reader
		skipLen  int64
		expected string
	}{
		{bytes.NewBuffer([]byte("")), 0, ""},
		{bytes.NewBuffer([]byte("")), 1, ""},
		{bytes.NewBuffer([]byte("abc")), 0, "abc"},
		{bytes.NewBuffer([]byte("abc")), 1, "bc"},
		{bytes.NewBuffer([]byte("abc")), 2, "c"},
		{bytes.NewBuffer([]byte("abc")), 3, ""},
		{bytes.NewBuffer([]byte("abc")), 4, ""},
	}
	for i, testCase := range testCases {
		r := NewSkipReader(testCase.src, testCase.skipLen)
		b, err := goioutil.ReadAll(r)
		if err != nil {
			t.Errorf("Case %d: Unexpected err %v", i, err)
		}
		if string(b) != testCase.expected {
			t.Errorf("Case %d: Got wrong result: %v", i, string(b))
		}
	}
}

func TestSameFile(t *testing.T) {
	f, err := goioutil.TempFile("", "")
	if err != nil {
		t.Errorf("Error creating tmp file: %v", err)
	}
	tmpFile := f.Name()
	f.Close()
	defer os.Remove(f.Name())
	fi1, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Error Stat(): %v", err)
	}
	fi2, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Error Stat(): %v", err)
	}
	if !SameFile(fi1, fi2) {
		t.Fatal("Expected the files to be same")
	}
	if err = goioutil.WriteFile(tmpFile, []byte("aaa"), 0644); err != nil {
		t.Fatal(err)
	}
	fi2, err = os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Error Stat(): %v", err)
	}
	if SameFile(fi1, fi2) {
		t.Fatal("Expected the files not to be same")
	}
}
