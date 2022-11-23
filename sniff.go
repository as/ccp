package main

import "github.com/as/log"

const (
	kindUnknown = "application/octet-stream"
	kindMP4     = "video/mp4"
	kindM4A     = "audio/mp4"
	kindFLAC    = "audio/x-flac"
	kindMP3     = "audio/x-mp3"
	kindWAV     = "audio/x-wav"
	kindJPEG    = "image/jpeg"
	kindPNG     = "image/png"
	kindMPEG    = "video/MP2T"
)

var kindTab = [...]struct {
	kind  string
	at    int
	magic string
}{
	{kind: kindMP4, at: 4, magic: "ftypmp42"},
	{kind: kindMP4, at: 4, magic: "ftypMSNV"},
	{kind: kindMP4, at: 4, magic: "ftypisom"},
	{kind: kindFLAC, magic: "fLaC"},
	{kind: kindMP3, magic: "\xff\xfb"},
	{kind: kindMP3, magic: "\xff\xf3"},
	{kind: kindMP3, magic: "\xff\xf2"},
	{kind: kindMP3, magic: "ID3"},
	{kind: kindWAV, at: 8, magic: "WAVE"},
	{kind: kindM4A, at: 4, magic: "ftypM4A"},
	{kind: kindJPEG, at: 0, magic: "\xff\xd8\xff\xe0"},
	{kind: kindPNG, at: 0, magic: "\x89\x50\x4e\x47"},
	{kind: kindMPEG, at: 0, magic: "G"},
}

func sniffContent(head []byte) (kind string) {
	const minHeadLen = 16
	defer func() {
		log.Debug.F("sniffContent: %x %s", head, kind)
	}()
	if len(head) < minHeadLen {
		return kindUnknown
	}
	for _, k := range kindTab {
		if string(head)[k.at:k.at+len(k.magic)] == k.magic {
			return k.kind
		}
	}
	return kindUnknown
}
