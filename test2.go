package main
import (
	"net/url"
	"fmt"
)

func main(){
	x, _ := url.Parse("http://www.xyz.com/")
	fmt.Printf("%#v", x)
}