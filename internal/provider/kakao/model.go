package kakao

type searchResponse struct {
	Meta      responseMeta       `json:"meta"`
	Documents []responseDocument `json:"documents"`
	Code      *int               `json:"code,omitempty"`
}

type responseMeta struct {
	TotalCount    int  `json:"total_count"`
	PageableCount int  `json:"pageable_count"`
	IsEnd         bool `json:"is_end"`
}

type responseDocument struct {
	Title       string   `json:"title"`
	Contents    string   `json:"contents"`
	URL         string   `json:"url"`
	ISBN        string   `json:"isbn"`
	Datetime    string   `json:"datetime"`
	Authors     []string `json:"authors"`
	Publisher   string   `json:"publisher"`
	Translators []string `json:"translators"`
	Price       *int64   `json:"price"`
	SalePrice   *int64   `json:"sale_price"`
	Thumbnail   string   `json:"thumbnail"`
	Status      string   `json:"status"`
}

type errorResponse struct {
	Code      *int   `json:"code"`
	Message   string `json:"msg"`
	ErrorType string `json:"errorType"`
}
