package crypto

import (
	"reflect"
	"testing"
)

func TestAesHandler_Decrypt(t *testing.T) {
	type fields struct {
		Key       []byte
		BlockSize int
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &AesHandler{
				Key:       tt.fields.Key,
				BlockSize: tt.fields.BlockSize,
			}
			got, err := h.Decrypt(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decrypt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decrypt() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAesHandler_Encrypt(t *testing.T) {
	type fields struct {
		Key       []byte
		BlockSize int
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &AesHandler{
				Key:       tt.fields.Key,
				BlockSize: tt.fields.BlockSize,
			}
			got, err := h.Encrypt(tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("Encrypt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Encrypt() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAesHandler_padding(t *testing.T) {
	type fields struct {
		Key       []byte
		BlockSize int
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &AesHandler{
				Key:       tt.fields.Key,
				BlockSize: tt.fields.BlockSize,
			}
			if got := h.padding(tt.args.src); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("padding() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAesHandler_unPadding(t *testing.T) {
	type fields struct {
		Key       []byte
		BlockSize int
	}
	type args struct {
		src []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &AesHandler{
				Key:       tt.fields.Key,
				BlockSize: tt.fields.BlockSize,
			}
			if got := h.unPadding(tt.args.src); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unPadding() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBase64Decrypt(t *testing.T) {
	decrypt, err := Base64Decrypt("177yRvku660_0bQzFIqpFM2v4P5JgoL9lP35oyz5LNI=", "NGA2WtbjyWgfER8od5zZghewn4-Mbk2wdLZJoJRz9sN-eZrdS_ySw9qTs82vaV6f")
	if err != nil {
		return
	}
	t.Log(decrypt)
}

func TestBase64Encrypt(t *testing.T) {
	base64Decrypt, _ := Base64Encrypt("NuE7q6aLS4m_ad3FujywX-U9KI76B4jw5Q9fdS8gBvQ=", "Qmd2zcCyaG4bpZB4b1JJN4uMPXgaqrN1TFPBxC78ZTxvr9")
	t.Log(base64Decrypt)
	decrypt, _ := Base64Decrypt("NuE7q6aLS4m_ad3FujywX-U9KI76B4jw5Q9fdS8gBvQ=", base64Decrypt)
	t.Log(decrypt)
	//base64Decrypt, _ := Base64Encrypt("NuE7q6aLS4m_ad3FujywX-U9KI76B4jw5Q9fdS8gBvQ=", "hello sdfsdfs w!!!")
	//t.Log(base64Decrypt)
	//decrypt, _ := Base64Decrypt("NuE7q6aLS4m_ad3FujywX-U9KI76B4jw5Q9fdS8gBvQ=", base64Decrypt)
	//t.Log(decrypt)
}

func TestNewAesHnadler(t *testing.T) {
	type args struct {
		key       []byte
		blockSize int
	}
	tests := []struct {
		name string
		args args
		want *AesHandler
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewAesHnadler(tt.args.key, tt.args.blockSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAesHnadler() = %v, want %v", got, tt.want)
			}
		})
	}
}
