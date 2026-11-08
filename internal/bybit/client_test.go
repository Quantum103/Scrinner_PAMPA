package bybit

import (
	"encoding/json"
	"net/http"
)

func writeResult(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"retCode": 0,
		"retMsg": "OK",
		"result": result,
	})
}
