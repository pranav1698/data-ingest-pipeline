package pipeline

import (
	"os"
	"errors"
	"fmt"
	"gorm.io/gorm"

	"github.com/pranav1698/data-ingest-pipeline/fileUtil"
	"github.com/pranav1698/data-ingest-pipeline/database"
	"github.com/pranav1698/data-ingest-pipeline/table"
)

type IPipeline interface {
	NewPipeline() (*Pipeline, error)
	ProcessFile(string) error
	CheckFile(string) error
	GetDateFromFileName(string) string
	CheckColumnsInDatabase([]string, []string) (error)
}

type Pipeline struct  {
	db *gorm.DB
	iMetricRecord database.IMetricRecord
	iSiteRecord database.ISiteRecord
	iCsvData table.ICSVData
}

func (p *Pipeline) NewPipeline() (*Pipeline, error) {
	dataBase, err := database.GetDatabaseConnection()
	if err != nil {
		return nil, err
	}

	var mr database.IMetricRecord = &database.MetricRecord{}
	var sr database.ISiteRecord = &database.SiteRecord{}
	var csv table.ICSVData = &table.CSVData{}

	return &Pipeline{
		db: dataBase,
		iMetricRecord: mr,
		iSiteRecord: sr,
		iCsvData: csv,
	}, nil

}

func (p *Pipeline) ProcessFile(filename string) (error) {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	err = p.CheckFile(filename)
	if err != nil {
		return err
	}

	date := p.GetDateFromFileName(filename)

	err = p.iMetricRecord.CreateMetricsTable(p.db)
	if err != nil {
		return err
	}

	err = p.iSiteRecord.CreateSiteTable(p.db)
	if err != nil {
		return err
	}

	data, err := p.iCsvData.GetCSVData(filename)
	if err != nil {
		return err
	}

	headers, err := p.iCsvData.GetCSVHeaders(filename)
	if err != nil {
		return err
	}

	columns, err := p.iMetricRecord.GetColumnsOfMetricsTable(p.db)
	if err != nil {
		return  err
	}

	err = p.CheckColumnsInDatabase(headers, columns)
	if err != nil {
		return err
	}

	
	for _, record := range data {
		siteRecord := p.iSiteRecord.NewSiteRecord(record.TargetPage)
		site, err := p.iSiteRecord.InsertRecordInSitesTable(p.db, siteRecord)
		if err != nil {
			return err
		}

		metricRecord := p.iMetricRecord.NewMetricRecord(site.TargetPageId, date, record.IncomingLinks, record.LinkingSites)
		_, err = p.iMetricRecord.InsertRecordInMetricsTable(p.db, metricRecord)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) CheckFile(fileName string) (error) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	isExcel := util.CheckExtension(fileName)
	if !isExcel {
		err := errors.New("Not a Excel File, please provide an excel or csv file as input")
		return err
	}

	checkFormat := util.CheckFormat(fileName)
	if !checkFormat {
		err := errors.New("Please check that file adheres to predefined format, for e.g.: https___www.thisisbarry.com_-Top target pages-2022-08-01.csv")
		return err
	}

	return nil
}


func (p *Pipeline) GetDateFromFileName(fileName string) (string) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	
	date := util.GetDate(fileName)
	return date
}

func (p *Pipeline) CheckColumnsInDatabase(headers []string, columns []string) (error) {
	for _, columnHeader := range headers {
		if columnHeader == "Target page" {
			continue
		}
		flag := false

		for _, dbColumnHeader := range columns {
			if dbColumnHeader == columnHeader {
				flag = true
			}
		}
		
		if !flag {
			err := fmt.Errorf("%s not present in database.", columnHeader)
			return err
		}
		
	}

	return nil
}
