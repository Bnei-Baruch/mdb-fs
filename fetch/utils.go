package fetch

import (
	"net/http"
	"io/ioutil"
	"fmt"
	"log"
)

func LogHttpResponse(resp *http.Response) string {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[ERROR] ioutil.ReadAll: %+v", err)
	}

	msg := fmt.Sprintf("Unexpected response [%d - %s] %s",
		resp.StatusCode, http.StatusText(resp.StatusCode), resp.Status)
	log.Printf("[ERROR] %s\nresp.Body: %s\n", msg, body)

	return msg
}
