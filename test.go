package main
import (
	"github.com/franela/goreq"
	"fmt"
	"time"
)

const (
	USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/41.0.2272.76 Chrome/41.0.2272.76 Safari/537.36"
)

func main() {
	resp, _ := goreq.Request {Uri:"http://www.adicomgroup.com/",
		RedirectHeaders:true,
		MaxRedirects:20,
		Insecure: true,
		UserAgent: USER_AGENT,
		Compression:goreq.Deflate(),
		Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
		Timeout: time.Duration(60) * time.Second}.Do()

	fmt.Printf("%#v", resp.Response)
}
