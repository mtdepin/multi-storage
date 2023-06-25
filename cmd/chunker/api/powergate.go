package api

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	pb "github.com/textileio/powergate/v2/api/gen/powergate/user/v1"
	"google.golang.org/grpc"
	"io"
	"time"
)

type ColdCredential struct {
	token string
}

func NewColdCredential(token string) *ColdCredential {
	return &ColdCredential{token: token}
}

func (c *ColdCredential) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"X-ffs-Token":       c.token,
		"X-pow-admin-token": c.token,
	}, nil

}
func (c *ColdCredential) RequireTransportSecurity() bool {
	return false
}
func NewColdClient(host string, token string) (pb.UserServiceClient, error) {
	var opts = []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(NewColdCredential(token)),
	}
	conn, err := grpc.Dial(host, opts...)
	if err != nil && host != "" {
		fmt.Println("grpc connect error")
		return nil, err
	}
	return pb.NewUserServiceClient(conn), nil
}
func SendToPowerGate(host, token string, file io.Reader) (string, error) {
	ctx := context.Background()

	uc, err := NewColdClient(host, token)
	if err != nil {
		fmt.Println("err")
		return "", err
	}
	start := time.Now()
	req, err := uc.Stage(ctx)
	if err != nil {
		return "", err
	}
	fileBuffer := make([]byte, 1024*64)
	for {
		bytesRead, err := file.Read(fileBuffer)
		if err != nil && err != io.EOF {
			return "", err

		}
		sendErr := req.Send(&pb.StageRequest{Chunk: fileBuffer[:bytesRead]})
		if sendErr != nil {
			if sendErr == io.EOF {
				var noOp interface{}
				return "", req.RecvMsg(noOp)
			}
			return "", sendErr
		}
		if err == io.EOF {
			break
		}
	}
	result, err := req.CloseAndRecv()
	if err != nil {
		return "", err
	}

	log.Infof("pow-server upload cost %f s", time.Since(start).Seconds())
	in := &pb.ApplyStorageConfigRequest{Cid: result.Cid, OverrideConfig: true, HasOverrideConfig: true}
	jobId, err := uc.ApplyStorageConfig(ctx, in)
	if err != nil {
		return "", err
	}
	log.Info("filCold add success jobId", jobId)
	return result.Cid, nil
}

func GetDataFromCold(host, token, ci string) (io.Reader, error) {
	ctx := context.Background()
	client, err := NewColdClient(host, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	start := time.Now()
	defer func() {
		log.Infof("GetInfo takes %f seconds", time.Since(start).Seconds())
	}()
	in := &pb.GetRequest{Cid: ci}
	result, err := client.Get(ctx, in)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	go func() {
		for {
			res, err := result.Recv()
			if err == io.EOF {
				_ = writer.Close()
				break
			} else if err != nil {
				_ = writer.CloseWithError(err)
				break
			}
			_, err = writer.Write(res.GetChunk())
			if err != nil {
				_ = writer.CloseWithError(err)
				break
			}
		}
	}()

	return reader, nil
}
