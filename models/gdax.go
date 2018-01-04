package models

type GdaxBucket struct {
	Timestamp              int64
	Low, High, Open, Close float64
	Volume                 float64
}

type GdaxError struct {
	Message string `json:"message"`
}