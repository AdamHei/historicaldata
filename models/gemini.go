package models

const GeminiCollection = "gemini"

type GeminiOrder struct {
	Timestamp   int64  `json:"timestamp"`
	TimestampMs int64  `json:"timestampms"`
	TID         int    `json:"tid"`
	Price       string `json:"price"`
	Amount      string `json:"amount"`
	Exchange    string `json:"exchange"`
	Type        string `json:"type"`
}
