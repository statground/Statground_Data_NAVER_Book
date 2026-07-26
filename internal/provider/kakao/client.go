package kakao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"statground_naver_book_go/internal/bookmodel"
	"statground_naver_book_go/internal/envx"
	"statground_naver_book_go/internal/provider"
)

const (
	ProviderName       = "kakao"
	BookSearchURL      = "https://dapi.kakao.com/v3/search/book"
	defaultMaxBodySize = int64(2 << 20)
)

const (
	ErrorAuthFailed     = "auth_failed"
	ErrorInvalidRequest = "invalid_request"
	ErrorPermission     = "permission_denied"
	ErrorQuotaExhausted = "quota_exhausted"
	ErrorRateLimited    = "rate_limited"
	ErrorTimeout        = "timeout"
	ErrorNetwork        = "network"
	ErrorUnavailable    = "unavailable"
	ErrorContract       = "contract_error"
	ErrorUnknown        = "unknown"
)

type APIError struct {
	Category   string
	HTTPStatus int
	KakaoCode  int
	Retryable  bool
}

func (e *APIError) Error() string {
	if e == nil {
		return "kakao api request failed"
	}
	if e.KakaoCode != 0 {
		return fmt.Sprintf(
			"kakao api request failed category=%s http_status=%d kakao_code=%d",
			e.Category,
			e.HTTPStatus,
			e.KakaoCode,
		)
	}
	return fmt.Sprintf(
		"kakao api request failed category=%s http_status=%d",
		e.Category,
		e.HTTPStatus,
	)
}

func ErrorCategory(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Category
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout
	}
	return ErrorUnknown
}

func IsQuotaStop(err error) bool {
	category := ErrorCategory(err)
	return category == ErrorQuotaExhausted || category == ErrorRateLimited
}

type Config struct {
	APIKey       string
	Endpoint     string
	HTTPClient   *http.Client
	Timeout      time.Duration
	Attempts     int
	BackoffMin   time.Duration
	BackoffMax   time.Duration
	MaxBodyBytes int64
	Rand         *rand.Rand
	Sleep        func(context.Context, time.Duration) error
}

type inflightCall struct {
	done     chan struct{}
	response provider.SearchResponse
	err      error
}

type Client struct {
	apiKey       string
	endpoint     string
	httpClient   *http.Client
	timeout      time.Duration
	attempts     int
	backoffMin   time.Duration
	backoffMax   time.Duration
	maxBodyBytes int64
	rand         *rand.Rand
	sleep        func(context.Context, time.Duration) error

	mu       sync.Mutex
	cache    map[string]provider.SearchResponse
	inflight map[string]*inflightCall
}

func NewClient(config Config) (*Client, error) {
	config.APIKey = strings.TrimSpace(config.APIKey)
	if config.APIKey == "" {
		return nil, fmt.Errorf("missing required environment variable: KAKAO_REST_API_KEY")
	}
	if config.Endpoint == "" {
		config.Endpoint = BookSearchURL
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("invalid Kakao Book endpoint")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = defaultHTTPClient()
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Second
	}
	if config.Attempts <= 0 {
		config.Attempts = 3
	}
	if config.BackoffMin <= 0 {
		config.BackoffMin = 500 * time.Millisecond
	}
	if config.BackoffMax <= 0 {
		config.BackoffMax = 4 * time.Second
	}
	if config.BackoffMax < config.BackoffMin {
		config.BackoffMax = config.BackoffMin
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaultMaxBodySize
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	return &Client{
		apiKey:       config.APIKey,
		endpoint:     config.Endpoint,
		httpClient:   config.HTTPClient,
		timeout:      config.Timeout,
		attempts:     config.Attempts,
		backoffMin:   config.BackoffMin,
		backoffMax:   config.BackoffMax,
		maxBodyBytes: config.MaxBodyBytes,
		rand:         config.Rand,
		sleep:        config.Sleep,
		cache:        make(map[string]provider.SearchResponse),
		inflight:     make(map[string]*inflightCall),
	}, nil
}

func NewClientFromEnv() (*Client, error) {
	key, err := envx.Require("KAKAO_REST_API_KEY")
	if err != nil {
		return nil, err
	}
	return NewClient(Config{
		APIKey:       key,
		Endpoint:     envx.String("KAKAO_BOOK_API_ENDPOINT", BookSearchURL),
		Timeout:      durationSeconds("KAKAO_API_TIMEOUT_SECONDS", 20*time.Second),
		Attempts:     envx.Int("KAKAO_API_ATTEMPTS", 3),
		BackoffMin:   durationSeconds("KAKAO_API_BACKOFF_MIN", 500*time.Millisecond),
		BackoffMax:   durationSeconds("KAKAO_API_BACKOFF_MAX", 4*time.Second),
		MaxBodyBytes: int64(envx.Int("KAKAO_API_MAX_RESPONSE_BYTES", int(defaultMaxBodySize))),
	})
}

func (c *Client) Name() string {
	return ProviderName
}

func (c *Client) Search(ctx context.Context, request provider.SearchRequest) (provider.SearchResponse, error) {
	if c == nil {
		return provider.SearchResponse{}, fmt.Errorf("kakao client is nil")
	}
	request = normalizeRequest(request)
	if err := provider.ValidateSearchRequest(request); err != nil {
		return provider.SearchResponse{}, &APIError{
			Category:  ErrorInvalidRequest,
			KakaoCode: 0,
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := requestFingerprint(request)

	c.mu.Lock()
	if cached, ok := c.cache[key]; ok {
		c.mu.Unlock()
		return cloneResponse(cached), nil
	}
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-call.done:
			return cloneResponse(call.response), call.err
		case <-ctx.Done():
			return provider.SearchResponse{}, ctx.Err()
		}
	}
	call := &inflightCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	response, err := c.searchUncached(ctx, request)

	c.mu.Lock()
	call.response = cloneResponse(response)
	call.err = err
	if err == nil {
		c.cache[key] = cloneResponse(response)
	}
	delete(c.inflight, key)
	close(call.done)
	c.mu.Unlock()
	return response, err
}

func (c *Client) searchUncached(ctx context.Context, request provider.SearchRequest) (provider.SearchResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= c.attempts; attempt++ {
		response, retryable, err := c.searchOnce(ctx, request)
		if err == nil {
			return response, nil
		}
		lastErr = err
		if !retryable || attempt == c.attempts {
			return provider.SearchResponse{}, err
		}
		if err := c.sleep(ctx, c.retryDelay(attempt)); err != nil {
			return provider.SearchResponse{}, err
		}
	}
	return provider.SearchResponse{}, lastErr
}

func (c *Client) searchOnce(
	ctx context.Context,
	request provider.SearchRequest,
) (provider.SearchResponse, bool, error) {
	requestURL, err := url.Parse(c.endpoint)
	if err != nil {
		return provider.SearchResponse{}, false, &APIError{Category: ErrorContract}
	}
	query := requestURL.Query()
	query.Set("query", request.Query)
	query.Set("sort", request.Sort)
	query.Set("page", strconv.Itoa(request.Page))
	query.Set("size", strconv.Itoa(request.Size))
	if request.Target != "" {
		query.Set("target", request.Target)
	}
	requestURL.RawQuery = query.Encode()

	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return provider.SearchResponse{}, false, &APIError{Category: ErrorContract}
	}
	httpRequest.Header.Set("Authorization", "KakaoAK "+c.apiKey)
	httpRequest.Header.Set("Accept", "application/json")

	observer := attemptObserverFromContext(ctx)
	startedAt := time.Now()
	attemptToken := ""
	if observer != nil {
		attemptToken, err = observer.BeforeAttempt(ctx, AttemptInfo{
			Request:   request,
			StartedAt: startedAt,
		})
		if err != nil {
			return provider.SearchResponse{}, false, err
		}
	}
	finishAttempt := func(result AttemptResult) error {
		if observer == nil {
			return nil
		}
		result.CompletedAt = time.Now()
		result.Elapsed = result.CompletedAt.Sub(startedAt)
		return observer.AfterAttempt(ctx, attemptToken, result)
	}

	httpResponse, err := c.httpClient.Do(httpRequest)
	if err != nil {
		category, retryable := classifyTransportError(err)
		apiErr := &APIError{
			Category:  category,
			Retryable: retryable,
		}
		if observerErr := finishAttempt(AttemptResult{ErrorCategory: category}); observerErr != nil {
			return provider.SearchResponse{}, false, observerErr
		}
		return provider.SearchResponse{}, retryable, apiErr
	}
	defer httpResponse.Body.Close()

	body, err := readBounded(httpResponse.Body, c.maxBodyBytes)
	if err != nil {
		apiErr := &APIError{
			Category:   ErrorContract,
			HTTPStatus: httpResponse.StatusCode,
		}
		if observerErr := finishAttempt(AttemptResult{
			HTTPStatus:    httpResponse.StatusCode,
			ErrorCategory: ErrorContract,
		}); observerErr != nil {
			return provider.SearchResponse{}, false, observerErr
		}
		return provider.SearchResponse{}, false, apiErr
	}

	if httpResponse.StatusCode != http.StatusOK {
		apiErr := classifyHTTPError(httpResponse.StatusCode, body)
		if observerErr := finishAttempt(AttemptResult{
			HTTPStatus:    httpResponse.StatusCode,
			KakaoCode:     apiErr.KakaoCode,
			ErrorCategory: apiErr.Category,
		}); observerErr != nil {
			return provider.SearchResponse{}, false, observerErr
		}
		return provider.SearchResponse{}, apiErr.Retryable, apiErr
	}

	var decoded searchResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		apiErr := &APIError{
			Category:   ErrorContract,
			HTTPStatus: httpResponse.StatusCode,
		}
		if observerErr := finishAttempt(AttemptResult{
			HTTPStatus:    httpResponse.StatusCode,
			ErrorCategory: ErrorContract,
		}); observerErr != nil {
			return provider.SearchResponse{}, false, observerErr
		}
		return provider.SearchResponse{}, false, apiErr
	}
	if decoded.Code != nil && *decoded.Code == -10 {
		apiErr := &APIError{
			Category:   ErrorQuotaExhausted,
			HTTPStatus: httpResponse.StatusCode,
			KakaoCode:  *decoded.Code,
		}
		if observerErr := finishAttempt(AttemptResult{
			HTTPStatus:    httpResponse.StatusCode,
			KakaoCode:     *decoded.Code,
			ErrorCategory: ErrorQuotaExhausted,
		}); observerErr != nil {
			return provider.SearchResponse{}, false, observerErr
		}
		return provider.SearchResponse{}, false, apiErr
	}
	documents := make([]bookmodel.BookDocument, 0, len(decoded.Documents))
	for _, document := range decoded.Documents {
		normalized, err := normalizeDocument(document)
		if err != nil {
			apiErr := &APIError{
				Category:   ErrorContract,
				HTTPStatus: httpResponse.StatusCode,
			}
			if observerErr := finishAttempt(AttemptResult{
				HTTPStatus:    httpResponse.StatusCode,
				ErrorCategory: ErrorContract,
			}); observerErr != nil {
				return provider.SearchResponse{}, false, observerErr
			}
			return provider.SearchResponse{}, false, apiErr
		}
		documents = append(documents, normalized)
	}
	response := provider.SearchResponse{
		TotalCount:    decoded.Meta.TotalCount,
		PageableCount: decoded.Meta.PageableCount,
		IsEnd:         decoded.Meta.IsEnd,
		Documents:     documents,
	}
	if observerErr := finishAttempt(AttemptResult{
		HTTPStatus:     httpResponse.StatusCode,
		Success:        true,
		DocumentsCount: len(documents),
	}); observerErr != nil {
		return provider.SearchResponse{}, false, observerErr
	}
	return response, false, nil
}

func classifyHTTPError(status int, body []byte) *APIError {
	kakaoCode := 0
	var decoded errorResponse
	if json.Unmarshal(body, &decoded) == nil && decoded.Code != nil {
		kakaoCode = *decoded.Code
	}
	if kakaoCode == -10 {
		return &APIError{
			Category:   ErrorQuotaExhausted,
			HTTPStatus: status,
			KakaoCode:  kakaoCode,
		}
	}
	switch status {
	case http.StatusUnauthorized:
		return &APIError{Category: ErrorAuthFailed, HTTPStatus: status, KakaoCode: kakaoCode}
	case http.StatusForbidden:
		return &APIError{Category: ErrorPermission, HTTPStatus: status, KakaoCode: kakaoCode}
	case http.StatusTooManyRequests:
		return &APIError{Category: ErrorRateLimited, HTTPStatus: status, KakaoCode: kakaoCode}
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return &APIError{
			Category:   ErrorUnavailable,
			HTTPStatus: status,
			KakaoCode:  kakaoCode,
			Retryable:  true,
		}
	case http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusUnprocessableEntity:
		return &APIError{Category: ErrorInvalidRequest, HTTPStatus: status, KakaoCode: kakaoCode}
	default:
		return &APIError{Category: ErrorUnknown, HTTPStatus: status, KakaoCode: kakaoCode}
	}
}

func classifyTransportError(err error) (string, bool) {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout, true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return ErrorTimeout, true
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "connection reset") ||
		strings.Contains(message, "connection refused") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "eof") {
		return ErrorNetwork, true
	}
	return ErrorNetwork, false
}

func normalizeRequest(request provider.SearchRequest) provider.SearchRequest {
	request.Query = strings.Join(strings.Fields(request.Query), " ")
	request.Sort = strings.ToLower(strings.TrimSpace(request.Sort))
	request.Target = strings.ToLower(strings.TrimSpace(request.Target))
	if request.Sort == "" {
		request.Sort = "accuracy"
	}
	if request.Page == 0 {
		request.Page = 1
	}
	if request.Size == 0 {
		request.Size = 10
	}
	return request
}

func requestFingerprint(request provider.SearchRequest) string {
	return strings.Join([]string{
		strings.ToLower(request.Query),
		request.Sort,
		request.Target,
		strconv.Itoa(request.Page),
		strconv.Itoa(request.Size),
	}, "\x1f")
}

func cloneResponse(response provider.SearchResponse) provider.SearchResponse {
	cloned := response
	cloned.Documents = make([]bookmodel.BookDocument, len(response.Documents))
	for i, document := range response.Documents {
		cloned.Documents[i] = document
		cloned.Documents[i].Authors = append([]string(nil), document.Authors...)
		cloned.Documents[i].Translators = append([]string(nil), document.Translators...)
		if document.PublishedAt != nil {
			publishedAt := *document.PublishedAt
			cloned.Documents[i].PublishedAt = &publishedAt
		}
		if document.ListPrice != nil {
			listPrice := *document.ListPrice
			cloned.Documents[i].ListPrice = &listPrice
		}
		if document.SalePrice != nil {
			salePrice := *document.SalePrice
			cloned.Documents[i].SalePrice = &salePrice
		}
	}
	return cloned
}

func readBounded(reader io.Reader, maxBytes int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response exceeds configured limit")
	}
	return body, nil
}

func (c *Client) retryDelay(attempt int) time.Duration {
	delay := c.backoffMin
	for i := 1; i < attempt; i++ {
		if delay >= c.backoffMax/2 {
			delay = c.backoffMax
			break
		}
		delay *= 2
	}
	if delay > c.backoffMax {
		delay = c.backoffMax
	}
	jitterRange := delay / 4
	if jitterRange <= 0 {
		return delay
	}
	if c.rand == nil {
		return delay
	}
	return delay + time.Duration(c.rand.Int63n(int64(jitterRange)))
}

func durationSeconds(name string, fallback time.Duration) time.Duration {
	seconds := envx.Float(name, fallback.Seconds())
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds * float64(time.Second))
}

func defaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
