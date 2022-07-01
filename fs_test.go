package main

import "testing"

func TestCommonPrefix(t *testing.T) {
	/*
		NOTE(as): This works poorly if the prefix differs across schemes and hosts
		so a file pair below would have a common prefix "/dir".

		gs://x/dir/file0
		s3://y/dir/file1
	*/
	for i, tc := range []struct {
		want string
		list []string
	}{
		{"/a/b", []string{
			"/a/b/foo/1",
			"/a/b/foo/2",
			"/a/b/foo/3/4/5",
			"/a/b/bar/1",
			"/a/b/bar/2/3",
		},
		},
		{"/a", []string{
			"/a/b/foo/1",
			"/a/b/foo/2",
			"/a/b/foo/3/4/5",
			"/a/b/bar/1",
			"/a/b/bar/2/3",
			"/a/c/bar/2/3",
		},
		},
		{"/a", []string{
			"gs://b/a/c/bar/2/3",
			"gs://b/a/b/foo/1",
			"gs://b/a/b/foo/2",
			"s3://b/a/b/foo/3/4/5",
			"s3://b/a/b/bar/1",
			"s3://b/a/b/bar/2/3",
			"gs://b/a/c/bar/2/3",
		},
		},
		{"/x", []string{
			"gs://a/x/c/bar/2/3",
			"s3://b/x/b/bar/1",
			"s3://c/x/b/bar/2/3",
		},
		},
		{"/", []string{
			"gs://a/x/c/bar/2/3",
			"s3://b/y/b/bar/1",
			"s3://c/x/b/bar/2/3",
		},
		},
	} {
		files := []Info{}
		for _, f := range tc.list {
			u := uri(f)
			files = append(files, Info{URL: &u})
		}
		have := commonPrefix(files...)
		if have != tc.want {
			t.Fatalf("test %d: bad common prefix: have %q want %q", i, have, tc.want)
		}
	}
}
