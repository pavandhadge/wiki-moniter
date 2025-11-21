package types

type WikimediaEvent struct {
	Meta struct {
		Topic string `json:"topic"`
	} `json:"meta"`
	Wiki  string `json:"wiki"`
	User  string `json:"user"`
	Bot   bool   `json:"bot"`
	Type  string `json:"type"`
	Title string `json:"title"`
}
