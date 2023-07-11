package database

import (
	"log"

	"gorm.io/gorm"
)

type IMetricRecord interface {
	NewMetricRecord(uint, string, int, int) *MetricRecord
	CreateMetricsTable(*gorm.DB) error
	GetColumnsOfMetricsTable(*gorm.DB) ([]string, error)
	InsertRecordInMetricsTable(*gorm.DB, *MetricRecord) (*MetricRecord, error)
}

type MetricRecord struct {
	TargetPageId uint `gorm:"column:TargetPageId;foreignKey:TargetPageId;references:TargetPageId"`
	Date string	`gorm:"column:Date"`
	IncomingLinks int `gorm:"column:Incoming links"`
	LinkingSites int `gorm:"column:Linking sites"`
}

func (mr *MetricRecord) NewMetricRecord(targetPageId uint, date string, incomingLinks int, linkingSites int) *MetricRecord {
	return &MetricRecord{
		TargetPageId: targetPageId,
		Date: date,
		IncomingLinks: incomingLinks,
		LinkingSites: linkingSites,
	}
}

func (mr *MetricRecord) CreateMetricsTable(db *gorm.DB) (error) {
	migrator := db.Migrator()
	exists := migrator.HasTable(&MetricRecord{})

	if exists {
		log.Println("Metrics Table already present in the database")
	} else {
		log.Println("Metrics Table Created")
		db.Table("metrics").AutoMigrate(&MetricRecord{})
	}

	return nil
}

func (mr *MetricRecord) GetColumnsOfMetricsTable(db *gorm.DB) ([]string, error) {
	columns, err := db.Migrator().ColumnTypes("metrics")
	if err != nil {
		return nil, err
	}

	var columnName []string
	for _, column := range columns {
		columnName = append(columnName, column.Name())
	} 
	

	return columnName, nil
}

func (mr *MetricRecord) InsertRecordInMetricsTable(db *gorm.DB, metricRecord *MetricRecord) (*MetricRecord, error) {
	metric, err := mr.CheckRecordInMetricsTable(db, metricRecord.TargetPageId, metricRecord.Date)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			res := db.Table("metrics").Create(metricRecord)
			if res.Error != nil {
				return nil, res.Error
			}
			return metricRecord, nil
		}
		return nil, err
	}

	metric, err = mr.UpdateRecordInMetricsTable(db, metricRecord)
	if err != nil {
		return nil, err
	}

	return metric, nil
}

func (mr *MetricRecord) CheckRecordInMetricsTable(db *gorm.DB, targetPageId uint, date string) (*MetricRecord, error) {
	var metric MetricRecord
	result := db.Table("metrics").Where("TargetPageId = ?", targetPageId).First(&metric)
	if result.Error != nil {
		return nil, result.Error
	}

	return &metric, nil
}

func (mr *MetricRecord) UpdateRecordInMetricsTable(db *gorm.DB, metricRecord *MetricRecord) (*MetricRecord, error) {
	var metric MetricRecord
	result := db.Table("metrics").Model(&metric).Where("TargetPageId = ?", metricRecord.TargetPageId).Where("Date = ?", metricRecord.Date).Updates(map[string]interface{}{"`Incoming links`": metricRecord.IncomingLinks, "`Linking sites`": metricRecord.LinkingSites})
	if result.Error != nil {
		return nil, result.Error
	}

	return &metric, nil
}