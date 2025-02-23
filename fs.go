package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/as/log"
)

var (
	agent    = flag.String("A", "", "user agent")
	header   = flag.String("H", "", "http header with colon seperated value (like curl)")
	tmp      = flag.String("tmp", os.TempDir(), "temporary directory location")
	partsize = flag.Int("partsize", 0, "temporary file partition size")
	secure   = flag.Bool("secure", true, "(deprecated; see -insecure) disable https to http downgrade when using bucket optimizations")
	insecure = flag.Bool("insecure", false, "enable https to http downgrade when using bucket optimizations")

	slow    = flag.Bool("slow", false, "disable parallelism for same-file downloads using temp files (see tmp and partsize)")
	sign    = flag.Bool("s", false, "presign one or more files (s3 and gs) and output http urls")
	maxhttp = flag.Int("maxhttp", 24, "global max http connections allowed")

	recurse = flag.Bool("r", false, "assume input is a directory and attempt recursion")

	bs       = flag.Int("bs", 0, "block size for copy operation (zero means unbuffered)")
	dry      = flag.Bool("dry", false, "print (and unroll) ccp commands only; no I/O ops")
	test     = flag.Bool("test", false, "open and create files, but do not read or copy data")
	quiet    = flag.Bool("q", false, "dont print any progress output")
	flaky    = flag.Bool("flaky", false, "treat i/o errors as non-fatal")
	debug    = flag.Bool("debug", false, "print debug logs")
	acl      = flag.String("acl", "", "apply this acl to the destination, e.g.: private, public-read, public-read-write, aws-exec-read")
	deadband = flag.Duration("deadband", 200*time.Second, "for copies, the non-cumulative duration of no io in the process (read+write) after which ccp emits a fatal error (zero means no timeout)")

	ls         = flag.Bool("ls", false, "list the source files or dirs")
	cat        = flag.Bool("cat", false, "concatenate the source files into one file (automatically enabled if dst is stdout)")
	appendonly = flag.Bool("append", false, "attempt to append to the dst instead of overwriting (filesystem only)")

	rel       = flag.Bool("rel", false, "ls omits scheme and bucket")
	stdinlist = flag.Bool("l", false, "treat stdin as a list of sources instead of data")

	seek     = flag.Int("seek", 0, "source file byte offset to start reading from")
	count    = flag.Int("count", 0, "source file bytes to read")
	version  = flag.Bool("v", false, "print version and exit")
	hashname = flag.String("hash", "", "hashes outgoing data (md5|sha1|sha256|sha384|sha512)")
	nogc     = flag.Bool("nogc", false, "dont delete temporary files (debugging only)")
	nosort   = flag.Bool("nosort", false, "this is a test flag that disables sorting of partition workers; used for debugging only")
	ipv4     = flag.Bool("4", false, "forces layer3 ipv4 for s3/http/https files")
	http1    = flag.Bool("1", false, "disables http2 support for all connections")
	maxmem   = flag.Int("maxmem", 32*1024*1024, "for http without -slow the maximum size at which a block0 will be created in memory instead of the disk; increasing this can reduce latency on slow disk backed storage at the expense of memory utilization")

	maxretry = flag.Int("retry", 3, "number of times ccp will retry an http download at a block level instead of terminating when the initial connection fails with a tcp reset")

	del = flag.Bool("d", false, "delete the file provided as the argument (currently not recursive)")

	limitRX = flag.Int("rxlimit", 0, "limit rx bandwidth (in MiB/s)")
	spin    = flag.Bool("spin", false, "disable thread release when reading from a very slow connection, this may cause 100% cpu usage if set to true")
)

var (
	errNotImplemented = errors.New("not yet implemented")
)

var ctx = context.Background()

var driver = map[string]interface {
	List(string) ([]Info, error)
	Delete(string) error
	Open(string) (io.ReadCloser, error)
	Create(string) (io.WriteCloser, error)
	Close() error
}{
	"s3":    &S3{ctx: ctx},
	"gs":    &GS{ctx: ctx},
	"file":  &OS{},
	"http":  &HTTP{ctx: ctx},
	"https": &HTTP{ctx: ctx},
	"":      &OS{},
}

var killc = make(chan os.Signal, 2)

func init() {
	signal.Notify(killc, syscall.SIGINT, syscall.SIGTERM)
}

func closeAll() {
	for _, fs := range driver {
		fs.Close()
	}
}

var hashes = map[string]func() hash.Hash{
	"md5":    md5.New,
	"sha1":   sha1.New,
	"sha256": sha256.New,
	"sha512": sha512.New,
}

func copyhash(dst io.Writer, src io.Reader) (n int64, sum string, err error) {
	var h hash.Hash
	new := hashes[*hashname]
	if new != nil {
		h = new()
		src = io.TeeReader(src, h)
	}
	if *limitRX <= 0 {
		n, err = io.Copy(tx{dst}, rx{src})
	} else {
		mbps := int(*limitRX) * 1024 * 1024
		n, err = io.Copy(tx{dst}, &rxlim{lim: mbps, rx: rx{src}})
	}
	if h != nil {
		sum = fmt.Sprintf("%x", h.Sum(nil))
	}
	return
}

func docp(src, dst string, ec chan<- work, donec chan bool) {
	if donec != nil {
		defer close(donec)
	}
	sfs := driver[uri(src).Scheme]
	dfs := driver[uri(dst).Scheme]

	{
		sfd, err := sfs.Open(src)
		if err != nil {
			ec <- work{src: src, dst: dst, err: fmt.Errorf("open src: %s: %w", src, err)}
			return
		}

		dfd, err := dfs.Create(dst)
		if err != nil {
			ec <- work{src: src, dst: dst, err: fmt.Errorf("create dst: %s: %w", dst, err)}
			return
		}
		sum := ""
		if !*test {
			_, sum, err = copyhash(dfd, sfd)
		}
		if err == nil {
			sfd.Close()
			if dst != "-" {
				if err = dfd.Close(); err != nil {
					err = fmt.Errorf("copy dst: %s: %w", dst, err)
				}
			}
		}
		ec <- work{src: src, dst: dst, sum: sum, err: err}
	}
}

func list(src ...string) {
	var fatal error
	for _, src := range src {
		sfs := driver[uri(src).Scheme]
		if sfs == nil {
			log.Fatal.F("src: scheme not supported: %s", src)
		}
		dir, err := sfs.List(src)
		if err != nil {
			log.Error.F("list error: %q: %v", src, err)
			fatal = err
			continue
		}
		if *rel {
			for _, f := range dir {
				fmt.Printf("%d\t%s\n", f.Size, f.Path)
			}
		} else {
			for _, f := range dir {
				fmt.Printf("%d\t%s\n", f.Size, f.URL)
			}
		}
	}
	if fatal != nil {
		log.Fatal.Add("err", fatal).Printf("")
	}
}

func dodelete(src ...string) {
	var fatal error
	for _, src := range src {
		sfs := driver[uri(src).Scheme]
		if sfs == nil {
			log.Fatal.F("src: scheme not supported: %s", src)
		}
		err := sfs.Delete(src)
		if err != nil {
			log.Error.F("delete error: %q: %v", src, err)
			fatal = err
			continue
		}
	}
	if fatal != nil {
		log.Fatal.Add("err", fatal).Printf("")
	}
}

var temps = []string{}

func main() {
	defer log.Trap()
	defer closeAll()

	flag.Parse()
	if *version {
		fmt.Println(Version)
		os.Exit(0)
	}
	temps = strings.Split(*tmp, ",")

	log.DebugOn = *debug
	if *ipv4 {
		dial := func(network, addr string) (conn net.Conn, err error) {
			conn, err = net.Dial(network, addr)
			line := log.Debug.Add("action", "dial", "network", network, "addr", addr)
			if err != nil {
				line.Error().Printf("connection failed: %s", err)
				return
			}
			line = line.Add("laddr", conn.LocalAddr().String())
			line = line.Add("raddr", conn.RemoteAddr().String())
			if strings.HasPrefix(conn.RemoteAddr().String(), "[") {
				line.Printf("skipping ipv6")
				conn.Close()
				return nil, errors.New("ipv6 disabled")
			}
			line.Printf("connected")
			return conn, err
		}
		if *http1 {
			log.Debug.Printf("bootstrap: ipv4: disable http2 completely")
			http.DefaultClient.Transport = &http.Transport{
				Dial:         dial,
				TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
			}
		} else {
			//
			log.Debug.Printf("bootstrap: ipv4: will try http1 with custom dialer")
			http.DefaultClient.Transport = &http.Transport{Dial: dial, ForceAttemptHTTP2: true}
		}
	} else if *http1 {
		log.Debug.Printf("bootstrap: disable http2 completely")
		http.DefaultClient.Transport = &http.Transport{
			TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
		}
	}

	sema = make(chan bool, *maxhttp)
	a := flag.Args()
	if *ls {
		list(a...)
		os.Exit(0)
	}
	if *del {
		dodelete(a...)
		os.Exit(0)
	}
	if *sign {
		type S interface {
			Sign(uri string) (string, error)
		}
		for _, src := range a {
			s, _ := driver[uri(src).Scheme].(S)
			if s != nil {
				su, err := s.Sign(src)
				if err == nil {
					src = su
				}
			}
			fmt.Println(src)
		}
		os.Exit(0)
	}

	if len(a) < 2 {
		log.Fatal.F("usage: ccp src... dst")
	}
	sfs, dfs := driver[uri(a[0]).Scheme], driver[uri(a[1]).Scheme]
	if sfs == nil {
		log.Fatal.F("src: scheme not supported: %s", a[0])
	}
	if dfs == nil {
		log.Fatal.F("dst: scheme not supported: %s", a[1])
	}

	var (
		list []Info
		err  error
	)
	if strings.HasSuffix(uri(a[0]).Path, "/") {
		// If it ends in a slash, its obviously a directory
		// and recursion is implied.
		*recurse = true
	}
	if a[0] == "-" && *stdinlist {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			info := Info{}
			v := strings.Split(sc.Text(), "\t")
			if len(v) > 1 {
				info.Size, _ = strconv.Atoi(v[0])
				v[0] = v[1]
			}
			u := uri(v[0])
			info.URL = &u
			list = append(list, info)
		}
		a[0] = commonPrefix(list...)
		if len(list) == 1 {
			a[0] = path.Dir(a[0])
		}
	} else if *recurse {
		list, err = sfs.List(a[0])
		line := log.Error.Add("action", "list", "src", a[0])
		if err != nil {
			if *flaky {
				line.Add("err", err).Printf("list error")
				u := uri(a[0])
				list = []Info{{URL: &u}}
			} else {
				line.Fatal().Add("err", err).Printf("")
			}
		}
	} else {
		list = []Info{}
		for _, file := range a[:len(a)-1] {
			u := uri(file)
			list = append(list, Info{URL: &u})
		}
	}

	ec := make(chan work, len(a)+len(list))
	n := 0
	lastarg := a[len(a)-1]
	if lastarg == "-" {
		*cat = true
	}
	var donec chan bool
	for i, src := range list {
		dst := src2dst(a[0], src.String(), lastarg) // TODO(as): bug, shouldnt be a[0]
		if *cat {
			donec = make(chan bool)
			dst = uri(lastarg)
			if i > 0 {
				*appendonly = true
			}
		}
		if *dry {
			fmt.Printf("ccp %q %q # %d\n", src, dst.String(), src.Size)
		} else {
			addquota(src.Size)
			go docp(src.String(), dst.String(), ec, donec)
			if donec != nil {
				if i+1 != len(list) {
					<-donec
				}
			}
			n++
		}
	}
	if *dry {
		os.Exit(0)
	}

	tick := time.NewTicker(time.Second).C
	stopmon := make(chan bool)
	fatal := make(chan string, 1)
	if *deadband != 0 {
		go monitor(stopmon, fatal, *deadband)
	}
	sizecheck := time.After(19 * time.Second)
	for i := 0; i < n; {
		select {
		case <-sizecheck:
			if getquota() != 0 {
				continue
			}
			cache.Range(func(key, value interface{}) bool {
				n, _ := value.(int)
				if *count != 0 {
					n = *count
				}
				addquota(n)
				return true
			})
		case msg := <-fatal:
			cleanup()
			log.Fatal.F("%s", msg)
		case sig := <-killc:
			cleanup()
			log.Fatal.F("trapped signal: %s", sig)
		case w := <-ec:
			i++
			line := log.Info.Add("action", "copy", "src", w.src, "dst", w.dst, "hashname", *hashname, "hash", w.sum, "err", w.err)
			if w.err != nil {
				line = log.Error.Add("status", "failed", "err", w.err)
				nerr++
				if *flaky {
					line.Printf("copy error: %s -> %s: %v", w.src, w.dst, w.err)
				} else {
					cleanup()
					line.Fatal().F("copy error: %s -> %s: %v", w.src, w.dst, w.err)
				}
			} else {
				line.Add("status", "done").Printf("")
			}
		case <-tick:
			progress(i, n)
		}
	}
	close(stopmon)

	progress(n, n)
	if nerr != 0 {
		cleanup()
		os.Exit(nerr)
	}
}

var tmpdir = sync.Map{}

// cleanup removes all registered and undeleted temporary files
func cleanup() {
	if *nogc {
		return
	}
	var wg sync.WaitGroup
	tmpdir.Range(func(key, value interface{}) bool {
		file, _ := key.(string)
		if file != "" {
			wg.Add(1)
			go func() {
				log.Debug.F("removing file %s", file)
				os.Remove(file)
				wg.Done()
			}()
		}
		return true
	})
	wg.Wait()
}

var nerr = 0
var txquota = int64(0)
var procstart = time.Now()

func monitor(done chan bool, fatal chan string, deadband time.Duration) {
	lastn := int64(0)
	lastio := time.Now()
	exit := func() bool {
		select {
		case <-done:
			return true
		default:
		}
		return false
	}
	for !exit() {
		time.Sleep(time.Second)
		rx := atomic.LoadInt64(&iostat.rx)
		tx := atomic.LoadInt64(&iostat.tx)
		n := rx + tx
		if n > lastn {
			lastio = time.Now()
			lastn = n
			continue
		}
		if time.Since(lastio) < deadband || exit() {
			continue
		}
		fatal <- fmt.Sprintf("io error: pipeline stalled, no rx/tx for %s after %0.3f MiB of io", deadband, float64(n)/1024/1024)
		return
	}
}

func addquota(n int) {
	atomic.AddInt64(&txquota, int64(n))
}
func getquota() int {
	return int(atomic.LoadInt64(&txquota))
}

func progress(done, total int) {
	rx := atomic.LoadInt64(&iostat.rx)
	tx := atomic.LoadInt64(&iostat.tx)
	dur := time.Since(procstart)
	bps := int64(0)
	if s := dur / time.Second; s > 0 {
		bps = tx / int64(s)
	}
	prog := int64(0)
	txquota := getquota()
	if txquota != 0 {
		prog = tx * 100 / int64(txquota)
		if prog > 100 {
			prog = 100
		}
	}

	if !*quiet {
		log.Info.Add(
			"rx", rx,
			"tx", tx,
			"total", txquota,
			"file.done", done,
			"file.total", total,
			"file.errors", nerr,
			"mbps", bps/(1024*1024),
			"progress", prog,
			"uptime", dur.Seconds(),
		).Printf("")
	}
}

type work struct {
	err           error
	src, dst, sum string
}

func src2dst(prefix, src, dst string) url.URL {
	su := uri(src)
	du := uri(dst)
	su.Path = strings.TrimPrefix(su.Path, uri(prefix).Path)
	du.Path = path.Join(du.Path, su.Path)
	return du
}

type Info struct {
	// invariant: *url.URL is never nil
	*url.URL
	Size int
}

func prefix(path string) string {
	n := strings.IndexAny(path, "*?[")
	if n > 0 {
		path = path[:n]
	}
	n = strings.LastIndex(path, "/")
	if n > 0 {
		path = path[:n]
	}
	return path
}

func uri(s string) url.URL {
	u, _ := url.Parse(s)
	if u == nil {
		return url.URL{}
	}
	return *u
}

func paths(file ...Info) (p []string) {
	for _, v := range file {
		p = append(p, v.Path)
	}
	return p
}

func commonPrefix(file ...Info) string {
	if len(file) == 0 {
		return ""
	}
	list := paths(file...)
	min := strings.Split(list[0], "/")
	for _, p := range list {
		if len(min) == 0 {
			break
		}
		a := strings.Split(p, "/")
		if len(a) < len(min) {
			a, min = min, a
		}
		n := 0
		for ; n < len(min) && min[n] == a[n]; n++ {
		}
		min = min[:n]
	}
	return path.Join(append([]string{"/"}, min...)...)
}
