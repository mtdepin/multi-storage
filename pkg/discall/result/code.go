package result

// Error codes
const (
	// Unknown storageerror code
	ErrUnknown                = 0
	STATE_CODE_SUCCESS    int = 200
	STATE_CODE_ACCEPTED   int = 201
	STATE_CODE_NO_CONTENT int = 203

	//已存在，或者已经创建
	STATE_CODE_EXISTED int = 300

	/********************4xx 请求错误**********************************/
	/**
	 * 错误请求
	 */
	STATE_CODE_BAD_REQUEST int = 400

	/**
	 * 没有通过通过验证
	 */
	STATE_CODE_UN_AUTENTICATION int = 401
	/**
	 * 参数不能为空
	 */
	STATE_CODE_PARAMETER_ERROR_NULL int = 402
	/**
	 * 资源禁止访问
	 */
	STATE_CODE_RESOURCE_FORBIDDEN int = 403

	/**
	 * 实体无法找到
	 */
	STATE_CODE_ITEM_NOT_FIND int = 404
	/**
	 * 方法不允许访问
	 */
	STATE_CODE_METHOD_NOT_ALLOW int = 405
	/**
	 * 请求的资源的内容特性无法满足请求头中的条件，因而无法生成响应实体。
	 */
	STATE_CODE_NOT_ACCEPTABLE int = 406
	/**
	 * 实体已经存在
	 */
	STATE_CODE_ITEM_EXITS int = 407

	/**
	 * 订单已经锁定
	 */
	STATE_CODE_ORDER_APPLY_LOCKER int = 408

	/**
	 * 订单已经提交，正在处理
	 */
	STATE_CODE_ORDER_APPLY_SUBMIT int = 409

	/**
	 * 并发处理错误，重复提交订单
	 */
	STATE_CODE_CONCURRENT_PROCESS_ERROR int = 410

	/**
	 * 签名错误
	 */
	STATE_CODE_SINGATURE_ERROR int = 411
	/**
	 * 先觉得条件未满足
	 */
	STATE_CODE_PREREQUISITE_NOT_SUPPORT int = 412
	/**
	 * 请求内容长度超过限制
	 */
	STATE_CODE_REQUEST_ENTITY_TOO_LARGE int = 413
	/**
	 * 请求格式不支持
	 */
	STATE_CODE_MEDIA_NOT_SUPPORT int = 415

	/**
	 * 订单冲突
	 */
	STATE_CODE_ORDER_CONCURRENT_LIMIT int = 441

	/**
	 * 用户未授权
	 */
	STATE_CODE_USER_UN_AUTHORIZED int = 442

	/**
	 * 订单超过极限
	 */
	STATE_CODE_ORDER_EXCEED_MAX int = 443

	/**
	 * 解析错误
	 */
	STATE_CODE_PARSER_ERROR int = 445
	/**
	 * 用户登陆已经过期
	 */
	STATE_CODE_EXECUTE_USER_EXPIRATION_ERROR int = 446

	ERR_READING_REQ_BODY int = 447

	/*****************5xxx 是服务错误*******************************************/
	/**
	 * 服务器内部处理错误
	 */
	STATE_CODE_SEVER_INNER_ERROR int = 500
	/**
	 * 不执行
	 */
	STATE_CODE_EXECUTE_PROCESS_ERROR int = 501
	/**
	 * 执行失败
	 */
	STATE_CODE_EXECUTE_FAIL_ERROR int = 502
	/**
	 * 服务不可获得
	 */
	STATE_CODE_SERVICE_UNAVAILABLE int = 503
	/**
	 * 请求其他代理超时
	 */
	STATE_CODE_GATEWAY_TIMEOUT int = 504

	/**
	 * 协议版本不支持
	 */
	STATE_CODE_VERSION_NOT_SUPPORT int = 505
	/**
	 * 没有充值的资源
	 */
	STATE_CODE_INSUFFICIENT_RESOURCE int = 507
	/**
	 * 请求容灾限制
	 */
	STATE_CODE_LIMITED_EXCEEDED int = 509

	// ALLIANCE_ORG_STATUS_OPEN is available
	ALLIANCE_ORG_STATUS_OPEN int = 1
	// CHANNEL_STATUS_OPEN
	CHANNEL_STATUS_OPEN int = 1
)
