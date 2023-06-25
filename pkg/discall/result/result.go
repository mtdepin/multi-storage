package result

type Code int
type RpcResult struct {
	Code    int         `json:"code"`    //status
	Data    interface{} `json:"data"`    //data
	Message string      `json:"message"` //message
	Success bool        `json:"success"` //data
}
