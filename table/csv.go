package table

import (
	"os"
	_ "encoding/csv"

	"github.com/gocarina/gocsv"
)

type CSVData struct {
	TargetPage string `csv:"Target page"`
	IncomingLinks int `csv:"Incoming links"`
	LinkingSites int `csv:"Linking sites"`
}

type ICSVData interface {
	GetCSVData(string) ([]CSVData, error)
	GetCSVHeaders(string) ([]string, error)
}

func (csd *CSVData) GetCSVData(filename string) ([]CSVData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data []CSVData
	if err := gocsv.UnmarshalFile(file, &data); err != nil {
		return nil, err
	}

	return data, nil
}

func (csd *CSVData) GetCSVHeaders(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// reader := csv.NewReader(file)

	// var data []*CSVData
	// if err := gocsv.UnmarshalCSV(reader, &data); err != nil {
	// 	return nil, err
	// }

	// headers := gocsv.Headers(&data[0])
	// return headers

	csvReader := gocsv.DefaultCSVReader(file)
	headers, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	return headers, nil

}