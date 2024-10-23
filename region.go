package main

import (
	"bufio"
	_ "embed"
	"strings"
)

//go:embed aws_regions.txt
var regionlist string

var regions = func() (list []string) {
	sc := bufio.NewScanner(strings.NewReader(regionlist))
	for sc.Scan() {
		if !strings.HasPrefix(sc.Text(), "#") {
			list = append(list, sc.Text())
		}
	}
	return
}

func fastregion(bucket string) string {
	return "" // TODO
}
