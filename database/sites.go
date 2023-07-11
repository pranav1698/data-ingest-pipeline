package database

import (
	"log"

	"gorm.io/gorm"
)

type ISiteRecord interface {
	NewSiteRecord(string) *SiteRecord
	CreateSiteTable(*gorm.DB) error
	InsertRecordInSitesTable(*gorm.DB, *SiteRecord) (*SiteRecord, error)
}

type SiteRecord struct {
	TargetPageId uint `gorm:"column:TargetPageId;primary_key;auto_increment"`
	TargetPage string `gorm:"column:Target page"`
}

func (sr *SiteRecord) NewSiteRecord(targetPage string) *SiteRecord {
	return &SiteRecord{TargetPage: targetPage,}
}

func (sr *SiteRecord) CreateSiteTable(db *gorm.DB) (error) {
	migrator := db.Migrator()
	exists := migrator.HasTable(&SiteRecord{})

	if exists {
		log.Println("Sites Table already present in the database")
	} else {
		log.Println("Sites Table Created")
		db.Table("sites").AutoMigrate(&SiteRecord{})
	}

	return nil
}

func (sr *SiteRecord) InsertRecordInSitesTable(db *gorm.DB, siteRecord *SiteRecord) (*SiteRecord, error) {
	site, err := sr.CheckRecordInSitesTable(db, siteRecord.TargetPage)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			res := db.Table("sites").Create(siteRecord)
			if res.Error != nil {
				return nil, res.Error
			}
			return siteRecord, nil
		}
		
		return nil, err
	} 

	return site, nil
}

func (sr *SiteRecord) CheckRecordInSitesTable(db *gorm.DB, targetPage string) (*SiteRecord, error) {
	var site SiteRecord
	result := db.Table("sites").Where("`Target page` = ?", targetPage).First(&site)
	if result.Error != nil {
		return nil, result.Error
	}

	return &site, nil
}