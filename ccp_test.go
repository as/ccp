package main

import (
	"bytes"
	"crypto/sha1"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

func TestPartsize(t *testing.T) {
	t.Log("this test will take a long time to run; it is exhaustive")
	t.Log("this test must be run with the current working directory set to the repo")
	t.Log("ensure you run 'go install' as this test calls ccp from $PATH")
	fs := http.FileServer(http.Dir("./"))
	ts := httptest.NewServer(fs)
	defer ts.Close()
	url := ts.URL + "/rand.bin"
	t.Log(string(url))
	tab := map[string]string{}
	for i := 0; i < 30; i++ {
		for j := 0; i+j < 30; j++ {
			key := fmt.Sprintf("%d,%d", i, j)
			tab[key] = sum(i, j)
		}
	}

	var wg sync.WaitGroup
	for _, partsize := range []string{"1", "2", "3", "7", "14", "15", "17", "20", "21", "23", "29", "30", "31"} {
		t.Log("testing with partsize", partsize)
		for j := 0; j < 30; j++ {
			for i := 0; i+j < 30; i++ {
				i, j := i, j
				wg.Add(1)
				func() {
					defer wg.Done()
					seek, count := fmt.Sprint(i), fmt.Sprint(j)
					key := seek + "," + count
					cmd := exec.Command("ccp", "-spin", "-debug", "-nosort", "-partsize", partsize, "-seek", seek, "-hash", "sha1", "-count", count, url, "-")
					b := &bytes.Buffer{}
					cmd.Stderr = b
					out, err := cmd.Output()
					if err != nil {
						t.Log(b.String())
						t.Fatal(err)
					}
					n := strings.Index(string(b.String()), `hash":"`)
					n += len(`hash":"`)
					h, w := string(b.String()[n:n+40]), tab[key]
					if h != w {
						io.Copy(os.Stdout, b)
						t.Logf("want: %q", string(rand))
						t.Logf("have: %q", string(out))
						t.Fatalf("seek=%d count=%d partsize=%s have: %s want: %s", i, j, partsize, h, w)
					}
				}()
			}
		}
	}
	wg.Wait()
}

func sum(seek, count int) string {
	h := sha1.New()
	if count == 0 {
		count = len(rand) - seek
	}
	if seek+count > len(rand) {
		count = len(rand) - seek
	}
	io.Copy(h, io.NewSectionReader(bytes.NewReader(rand), int64(seek), int64(count)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

//go:embed rand.bin
var rand []byte
