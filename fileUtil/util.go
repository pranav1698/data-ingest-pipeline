package fileUtil

import (
	"path/filepath"
	"regexp"
)

type IFileUtil interface {
	CheckExtension(string) bool
	CheckFormat(string) bool
	GetDate(string) string
}

type FileUtil struct {

}

func (fu *FileUtil) CheckExtension(filename string) bool {
	extension := filepath.Ext(filename)
	
	switch extension {
	case ".csv":
		return true
	case ".xls":
		return true
	case ".xlsx":
		return true
	}

	return false
}

func (fu *FileUtil) CheckFormat(filename string) bool {
	baseName := filepath.Base(filename)
	regexPattern := `^https___www.thisisbarry.com_-Top target pages-\d{4}-\d{2}-\d{2}\.([a-z]+)$`

	r, _ := regexp.Compile(regexPattern)

	match := r.MatchString(baseName)
	return match
}

func (fu *FileUtil) GetDate(filename string) string {
	baseName := filepath.Base(filename)
	datePattern := `\d{4}-\d{2}-\d{2}`

	reg := regexp.MustCompile(datePattern)
	date := reg.FindString(baseName)

	return date
}
