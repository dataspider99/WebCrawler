package main
import (
	"flag"
	"time"
	"net/url"
	"os"
	"bufio"
	"fmt"
	"regexp"
	"encoding/csv"
	"net"
	"strconv"
	"sync"
	"strings"
	"golang.org/x/net/publicsuffix"
	"github.com/franela/goreq"
	"github.com/moovweb/gokogiri"
	"github.com/moovweb/gokogiri/xpath"
	"runtime"
	"gopkg.in/mgo.v2"
	"path"
	"github.com/streamrail/concurrent-map"
)

var urlsFile string
var concurrency int
var validate_domains bool
var download_content bool
var store_headers bool
var mongo_server string
var timeout int
var mongo_collection string

var reww *regexp.Regexp
var redots *regexp.Regexp
var respaces *regexp.Regexp
var rehttp *regexp.Regexp
var rehttps *regexp.Regexp

type domain_info struct {
	Domain string
	IsValid bool
	HasRobotTxt bool
}
var urls_maps cmap.ConcurrentMap

type url_info struct {
	ParsedUrl string
	MainUrl string
	Content string
	ContentType string
	ProtocolStatus int
	Domain string
	DomainValid string
	Redirected bool
	RedirectTo string
	ContentQualification int
	RetryCount int
	Time time.Time
	Depth int

}


func init() {
	flag.StringVar(&urlsFile, "urls", "/home/tarun/IdeaProjects/GoScraper/urls2.txt", "File for input urls")
	flag.IntVar(&concurrency, "concurrency", 30, "How many connections to use")
	flag.BoolVar(&validate_domains, "domain", true, "Validate domains or not?")
	flag.BoolVar(&download_content, "content", true, "Download content of the url")
	flag.BoolVar(&store_headers, "headers", true, "Store response headers")
	flag.StringVar(&mongo_server, "server", "50.112.92.232", "Mongo server to dump results")
	flag.StringVar(&mongo_collection, "collection", "urls", "Mongo collection to dump data")

	flag.IntVar(&timeout, "timeout", 60, "Timeout for downloads of url")
	goreq.SetConnectTimeout(time.Second * 30)
	urls_maps = cmap.New()
	respaces = regexp.MustCompile(`^\s+|\s+$`)
	reww = regexp.MustCompile(`^(?i)\s*(ww|wwww)\s*\.`)
	redots = regexp.MustCompile(`\s*[.·]+\s*`)
	rehttp = regexp.MustCompile(`^(?i)\s*ht[tp]{2,3}\s*[:.;,：]?\s*[\\/]+(ht[tp]{2,3}[:.;,：]?([\\/]+)?)?`)
	rehttps = regexp.MustCompile(`^(?i)\s*ht[tp]{2,3}s\s*[:.,;：]?\s*[\\/]+(ht[tp]{2,3}s?[:.;,：]?([\\/]+)?)?`)

	if false {
		u, _ := get_clean_url(`http://http.//www.schad-automation.com`)

		fmt.Println(u)
		os.Exit(0)
	}
}

const (
	URL_FETCHED = 1
	URL_NOT_FETCHED = 0
	URL_NOT_ALLOWED = 2

)

const (
	USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/41.0.2272.76 Chrome/41.0.2272.76 Safari/537.36"
)
type Urltest struct {
	OrgURL string
	CleanedURL string    `json:"_id" bson:"_id"`
	DomainName string
	DomainValid bool
	HasRedirect bool
	ContentType string
	SourceDomain string
	DestinationDomain string
	EffectiveURL string
	Errors []string
	Index int
	Content string
	ParseFailed bool
	Status string
	StatusCode int
	ContentQualification int
	RetryCount int
	Time time.Time
	Depth int
	OutLinks [][]string
	FetchStatus int
}

type DownloadStats struct {
	in_download_queue int
	retry_count int
	download_failed int
	http_errors int
	parsing_errors int
	outlinks_count int
	sites_with_outlinks int
	download_completed int
	failed_domains int
	completed_domains int
	duplicate_urls int
	already_downloaded int
}

var stats = &DownloadStats{}

func get_clean_url(u string) (clean_url string, err error) {
	u = rehttp.ReplaceAllString(u, "http://")
	u = rehttps.ReplaceAllString(u, "https://")
	parsed, err := url.Parse(u)
	if err != nil || parsed.Scheme == "" {
		//There might be an http err
		parsed, err = url.Parse("http://" + u)
	}
	if err != nil {
		return "", err
	}

	parsed.Fragment = ""
	if parsed.Scheme == "" {
		parsed.Scheme = "http"
	}
	parsed.Host = redots.ReplaceAllString(parsed.Host, ".")
	parsed.Host = respaces.ReplaceAllString(parsed.Host, "")
	parsed.Host = reww.ReplaceAllString(parsed.Host, "www.")
	parsed.Host = strings.Replace(parsed.Host, ",", ".", -1)
	parsed.Host = strings.ToLower(parsed.Host)

	return parsed.String(), err
}

const MAX_RETRIES = 0

func (info *Urltest) AppendError(error string) {
	info.Errors = append(info.Errors, error)
}

func urljoin(base_url string, new_url string) (string) {
	new_url = strings.TrimSpace(new_url)
	if new_url == "" {
		return base_url
	}

	bu, err := url.Parse(base_url)
	if err != nil {
		return new_url
	}
	nu, err := url.Parse(new_url)
	if (err == nil && nu.Scheme != "") {
		return new_url
	}
	var new_path string
	if (nu != nil) {
		new_path = path.Join(bu.Path, nu.Path)
	} else {
		new_path = path.Join(bu.Path, new_url)
	}

	if !strings.HasPrefix(new_path, "/") {
		new_path = "/" + new_path
	}

	return bu.Scheme + "://" + bu.Host + new_path
}

func check_url_download_needed(info Urltest) (bool){
	cache_url := strings.Replace(info.CleanedURL, "//www.", "//", -1)
	_, inqueue := urls_maps.Get(cache_url)

	count, _ := coll.FindId(info.CleanedURL).Count()
	switch true {
	case inqueue:
		stats.duplicate_urls++
		return false
	case count > 0:
		stats.already_downloaded++
		urls_maps.Set(cache_url, info.CleanedURL)
		return false
	}

	return true
}

func download_urls(download_urls chan *Urltest, download_completed chan <- *Urltest) {
	sem := make(chan bool, concurrency)
	for info := range (download_urls) {
		sem <- true
		go func(info *Urltest) {
			defer func() { <-sem }()
			stats.in_download_queue++
			cache_url := strings.Replace(info.CleanedURL, "//www.", "//", -1)
			println("Begining download for - " + info.CleanedURL)
			urls_maps.Set(cache_url, info.CleanedURL)
			resp, err := goreq.Request{Uri:info.CleanedURL,
				RedirectHeaders:true,
				MaxRedirects:20,
				UserAgent: USER_AGENT,
				Compression:goreq.Gzip(),
				Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
				Timeout: time.Duration(timeout) * time.Second}.Do()

			if err != nil {
				println("Error occured while downloading url " + err.Error() + info.CleanedURL)
				info.Errors = append(info.Errors, err.Error())
				info.RetryCount ++
				if (info.RetryCount <= MAX_RETRIES) {
					download_urls <- info
					stats.retry_count++
				}else {
					download_completed <- info
					stats.download_failed++
				}
				return
			} else {
				if (resp.StatusCode == 429 || resp.StatusCode == 500) {
					info.RetryCount++
					if (info.RetryCount <= MAX_RETRIES) {
						download_urls <- info
						stats.retry_count++
					} else {
						download_completed <- info
						stats.download_failed++
					}
					return
				} else {
					if (resp.StatusCode != 200) {
						println("Failed to download URL for - " + resp.Status + " " + info.CleanedURL)
						info.AppendError(resp.Status)
						stats.http_errors++
					}
					info.EffectiveURL = resp.Uri
					purl, _ := url.Parse(resp.Uri)
					dm, _ := publicsuffix.EffectiveTLDPlusOne(purl.Host)
					info.DestinationDomain = dm
					info.HasRedirect = !(resp.Uri == info.CleanedURL)
					info.ContentType = resp.Header.Get("Content-Type")
					info.Content, err = resp.Body.ToString()
					if err != nil {
						stats.parsing_errors++
						info.AppendError(err.Error())
					} else {
						doc, err := gokogiri.ParseHtml([]byte(info.Content))
						defer doc.Free()

						if err != nil {
							println("Failed to parse url - " + err.Error() + " " + info.CleanedURL)
							info.AppendError(err.Error())
							info.ParseFailed = true
							stats.parsing_errors++
						} else {
							links_xpath := xpath.Compile("//a[@href] | //frame[@src] | //iframe[@src]")
							root := doc.Root()
							if root == nil {
								stats.parsing_errors++
								info.ParseFailed = true
								println("Failed to get root element for - " + info.CleanedURL)
							} else {
								links, _ := root.Search(links_xpath)
								outlinks := make([] []string, 0)
								for _, link := range links {
									tagName := strings.ToLower(link.Name())
									var text, href string
									if tagName == "a" {
										href = link.Attr("href")
										text = strings.TrimSpace(link.Content())
									} else {
										href = link.Attr("src")
										text = ""
									}
									if href != "" {
										href = urljoin(info.EffectiveURL, href)
										outlinks = append(outlinks, []string{text, href})
									}
								}
								info.OutLinks = outlinks
								stats.outlinks_count += len(outlinks)
								if len(outlinks) > 0 {
									stats.sites_with_outlinks++
								}
							}
						}
					}
				}
				info.StatusCode = resp.StatusCode
				info.Status = resp.Status
				stats.download_completed++
				println("Download completed for - " + info.CleanedURL)
				download_completed <- info
			}
		}(info)
	}
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	println("Downloader finished. Exiting downloader code")
}

func verify_domain(urls <-chan *Urltest, domain_resolved chan <- *Urltest, download_url chan <- *Urltest, download_completed chan<- *Urltest, completed chan <- bool) {
	var wg sync.WaitGroup
	sem := make(chan bool, concurrency)

	for info := range (urls) {
		wg.Add(1)
		sem<-true
		go func(info *Urltest, wg *sync.WaitGroup) {

			defer func() {
				wg.Done()
				<-sem
			}()
			url_info, _ := url.Parse(info.CleanedURL)
			info.SourceDomain, _ = publicsuffix.EffectiveTLDPlusOne(url_info.Host)
			hostname := url_info.Host
			_, err := net.LookupHost(hostname)
			var err2 error
			if err != nil {
				time.Sleep(time.Millisecond * 50)
				//fmt.Println("Checking domain again - " + info.CleanedURL)
				_, err2 = net.LookupHost(hostname)
			}

			if err2 != nil {
				info.DomainValid = false
				info.Errors = append(info.Errors, err2.Error())
				stats.failed_domains++
				download_completed <- info
			}else {
				info.DomainValid = true
				stats.completed_domains++
				if check_url_download_needed(*info) {
					download_url <- info
				}else{
					//println("No need to download - " + info.CleanedURL)
				}
			}



			domain_resolved <- info
		}(info, &wg)

	}
	fmt.Println("Waiting for Domain verification completion")
	wg.Wait()
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	close(domain_resolved)

	completed <- true
}

func writer_result(writer *csv.Writer, domain_resolved <-chan *Urltest, completion chan <- bool) {
	for info := range domain_resolved {
		var domain_valid string
		if info.DomainValid {
			domain_valid = "true"
		}else {
			domain_valid = "false"
		}

		data := []string{
			info.OrgURL,
			info.CleanedURL,
			info.DomainName,
			domain_valid,
			strconv.FormatBool(info.HasRedirect),
			info.SourceDomain,
			info.DestinationDomain,
			info.EffectiveURL, strings.Join(info.Errors, "++"), strconv.Itoa(info.Index)}
		writer.Write(data)
	}

	fmt.Println("Writer completed")
	completion <- true
}

func print_stats(stats_channel chan bool) {
	//previous_complete := 0
	for {
		select {
		case _, ok := <-stats_channel:
			if !ok {
				fmt.Println("Stats channel closed")
				return
			}
			//rate := stats.completed_domains + stats.failed_domains - previous_complete
			//previous_complete = stats.completed_domains + stats.failed_domains
			//fmt.Println("Resolved domains in 1 sec = ", rate)
			fmt.Printf("%#v\n", stats)
			time.Sleep(time.Second * 30)
			stats_channel <- true
		default:
			break
		}
	}
}


func completed_download(download_complete chan *Urltest, completed chan <- bool) {
	for info := range download_complete {
		//fmt.Println("%#v", info)
		stats.in_download_queue--
		println("Document received for - " + info.CleanedURL)
		info.Time = time.Now().UTC()
		coll.Insert(&info)
	}

	println("All document completed")
	completed <- true
}

func get_mongo_connection() (*mgo.Session) {
	conn, _ := mgo.Dial(mongo_server)
	conn.SetMode(mgo.Monotonic, true)
	return conn
}

var conn *mgo.Session
var coll *mgo.Collection

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	conn = get_mongo_connection()
	coll = conn.DB("test2").C(mongo_collection)
	flag.Parse()
	if flag.NArg() > 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	path := urlsFile
	inFile, err := os.Open(path)
	csvfile, err2 := os.Create("output.csv")
	csvfile2, err3 := os.Create("output2.csv")

	defer inFile.Close()
	defer csvfile.Close()
	defer csvfile2.Close()

	if err != nil || err2 != nil || err3 != nil {
		print("Some error has occured", err, err2, err3)
		os.Exit(1)
	}


	writer := csv.NewWriter(csvfile)
	writer2 := csv.NewWriter(csvfile2)

	domain_resolver := make(chan *Urltest, concurrency)
	domain_resolved := make(chan *Urltest, concurrency)
	download_url := make(chan *Urltest, concurrency * 4)
	download_completed := make(chan *Urltest, concurrency * 2)
	completion := make(chan bool, 3)
	stats_channel := make(chan bool, 1)
	stats_channel <- true

	go print_stats(stats_channel)

	go verify_domain(domain_resolver, domain_resolved, download_url, download_completed, completion)
	go writer_result(writer2, domain_resolved, completion)
	go download_urls(download_url, download_completed)
	go completed_download(download_completed, completion)
	errorCount := 0
	cleaned := 0
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	line := 0
	for scanner.Scan() {
		org_url := scanner.Text()
		clean_url, err := get_clean_url(org_url)

		var data []string
		if err != nil {
			errorCount++
			fmt.Println(org_url, err)
			data = []string{org_url, clean_url, err.Error()}
		}else {
			if org_url  != clean_url {
				cleaned++
			}
			data = []string{org_url, clean_url, ""}
		}
		//writer.Write(data)
		print(data[1]+"\n")
		writer.Write([]string{clean_url})

		test := Urltest{CleanedURL:clean_url, OrgURL:org_url, Index: line}
		line++
		domain_resolver <- &test
	}

	fmt.Println("Waiting for resolutions to get over")
	println("Closing domain resolver")
	close(domain_resolver)
	writer.Flush()
	<-completion
	<-completion
	<-completion
	writer2.Flush()
	println("Closing download completed channel")
	close(download_completed)
	close(stats_channel)
	fmt.Println("Script completed. Errors=", errorCount, "Cleaned=", cleaned, "Passed", stats.completed_domains, "Failed=", stats.failed_domains)
	fmt.Printf("%#v\n", stats)

}


