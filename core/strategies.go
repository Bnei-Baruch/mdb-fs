package core

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

const (
	MDBStrategyAllPublic             = "all_public"
	MDBStrategyPublicOrSmallInMerkaz = "public_or_small_in_merkaz"
	MDBStrategyPreferredLanguage     = "preferred_language"
)

type MDBStrategy interface {
	Augment(*sql.DB, map[string]*FileRecord) error
}

func MakeMDBStrategy(name, params string) (MDBStrategy, error) {
	switch name {
	case MDBStrategyAllPublic:
		return NewAllPublicStrategy(), nil
	case MDBStrategyPublicOrSmallInMerkaz:
		return NewPublicOrSmallInMerkazStrategy(), nil
	case MDBStrategyPreferredLanguage:
		return NewPreferredLanguageStrategy(strings.Split(params, ",")), nil
	default:
		return nil, errors.Errorf("Unknown MDB strategy: %s", name)
	}
}

type SimpleQueryStrategy struct {
	query string
}

func NewSimpleQueryStrategy(query string) MDBStrategy {
	return &SimpleQueryStrategy{query: query}
}

func (s *SimpleQueryStrategy) Augment(db *sql.DB, idx map[string]*FileRecord) error {
	rows, err := db.Query(s.query)
	if err != nil {
		return errors.Wrap(err, "db.Query")
	}
	defer rows.Close()

	for rows.Next() {
		var id, size int64
		var b []byte
		err = rows.Scan(&id, &b, &size)
		if err != nil {
			return errors.Wrap(err, "rows.Scan")
		}

		sha1 := hex.EncodeToString(b)
		if r, ok := idx[sha1]; ok {
			r.MdbID = id
			r.MdbSize = size
		} else {
			idx[sha1] = &FileRecord{
				Sha1:      sha1,
				MdbID:     id,
				MdbSize:   size,
				LocalCopy: false,
			}
		}
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows.Err()")
	}

	return nil
}

type AllPublicStrategy struct{}

func NewAllPublicStrategy() MDBStrategy {
	return NewSimpleQueryStrategy(`select distinct f.id, f.sha1, f.size
from files f
	inner join files_storages fs on f.id = fs.file_id
where f.sha1 is not null
  and f.removed_at is null
  and f.published is true;`)
}

type PublicOrSmallInMerkazStrategy struct{}

func NewPublicOrSmallInMerkazStrategy() MDBStrategy {
	return NewSimpleQueryStrategy(`select distinct f.id, f.sha1, f.size
from files f
       inner join files_storages fs on f.id = fs.file_id
       inner join storages s on fs.storage_id = s.id and s.location = 'merkaz'
where f.sha1 is not null
  and f.removed_at is null
  and (f.published is true or f.type in ('image', 'text'));`)
}

type MDBFile struct {
	ID            int64
	Sha1          string
	Name          string
	Type          string
	Language      string
	VideoSize     sql.NullString
	MimeType      sql.NullString
	ContentUnitID int64
	Size          int64
}

type PreferredLanguageStrategy struct {
	Languages []string
}

func NewPreferredLanguageStrategy(Languages []string) MDBStrategy {
	return &PreferredLanguageStrategy{Languages: Languages}
}

func (s *PreferredLanguageStrategy) Augment(db *sql.DB, idx map[string]*FileRecord) error {
	// fetch all must have files
	// 47 | SOURCE
	// 48 | LIKUTIM
	query := `select distinct f.id, f.sha1, f.size 
from files f 
	inner join files_storages fs on f.id = fs.file_id 
	inner join content_units cu on f.content_unit_id = cu.id and cu.type_id in (47, 48)
where f.sha1 is not null 
	and f.removed_at is null 
	and f.published is true;`

	if err := NewSimpleQueryStrategy(query).Augment(db, idx); err != nil {
		return errors.Wrap(err, "fetch all must have")
	}

	// fetch all files in language
	// no need for lessons derivatives
	// 31 | KITEI_MAKOR
	// 46 | KTAIM_NIVCHARIM
	mdbFiles := make(map[string]*MDBFile)
	query = fmt.Sprintf(`select distinct f.id, f.sha1, f.name, f.size, f.type, f.mime_type, f.language, f.properties->>'video_size' as "video_size", f.content_unit_id 
from files f 
	inner join files_storages fs on f.id = fs.file_id 
	inner join content_units cu on f.content_unit_id=cu.id and cu.type_id not in (31, 46)
where f.sha1 is not null 
	and f.removed_at is null 
	and f.published is true 
	and f.language in ('%s');`, strings.Join(s.Languages, "','"))
	rows, err := db.Query(query)
	if err != nil {
		return errors.Wrap(err, "db.Query")
	}
	defer rows.Close()

	for rows.Next() {
		var f MDBFile
		var b []byte
		err = rows.Scan(&f.ID, &b, &f.Name, &f.Size, &f.Type, &f.MimeType, &f.Language, &f.VideoSize, &f.ContentUnitID)
		if err != nil {
			return errors.Wrap(err, "rows.Scan")
		}

		f.Sha1 = hex.EncodeToString(b)
		if _, ok := mdbFiles[f.Sha1]; !ok {
			mdbFiles[f.Sha1] = &f
		}
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows.Err()")
	}

	// find best files
	// [cu_id] -> [<lang, type>] -> best file
	groupedMdbFiles := make(map[int64]map[string]*MDBFile)
	for _, f := range mdbFiles {
		cuFiles, ok := groupedMdbFiles[f.ContentUnitID]
		if !ok {
			groupedMdbFiles[f.ContentUnitID] = make(map[string]*MDBFile)
			cuFiles = groupedMdbFiles[f.ContentUnitID]
		}

		langType := fmt.Sprintf("%s_%s", f.Language, f.Type)
		bestFile, ok := cuFiles[langType]
		if ok {
			if fileRank(f) > fileRank(bestFile) {
				cuFiles[langType] = f
			}
		} else {
			cuFiles[langType] = f
		}
	}

	// augment index
	for _, v := range groupedMdbFiles {
		for _, f := range v {
			if r, ok := idx[f.Sha1]; ok {
				r.MdbID = f.ID
				r.MdbSize = f.Size
			} else {
				idx[f.Sha1] = &FileRecord{
					Sha1:      f.Sha1,
					MdbID:     f.ID,
					MdbSize:   f.Size,
					LocalCopy: false,
				}
			}
		}
	}

	return nil
}

var videoSizeRankMap = map[string]int64{
	"":    1,
	"nHD": 1, // 640×360 (nHD)
	"sHD": 2, // 1280x480 (non standard)
	"HD":  3, // 1280×720 (HD)
	"FHD": 4, // 1920×1080 (FHD)
	"4k":  5, // 3840×2160 (4K UHD)
	"8k":  6, // 7680×4320 (8K UHD)
}

var videoMimeTypeRankMap = map[string]int64{
	"audio/mpeg":      1,
	"video/x-ms-wmv":  2,
	"video/x-msvideo": 3,
	"video/x-ms-asf":  4,
	"video/x-flv":     5,
	"video/quicktime": 6,
	"video/mpeg":      7,
	"video/mp4":       8,
}

var audioMimeTypeRankMap = map[string]int64{
	"audio/mp3":      0,
	"audio/mpeg":     0,
	"audio/x-wav":    1,
	"audio/aac":      2,
	"audio/midi":     3,
	"audio/x-ms-wma": 4,
}

func fileRank(f *MDBFile) int64 {
	switch f.Type {
	case "video":
		return videoFileRank(f)
	case "audio":
		return audioFileRank(f)
	default:
		return f.Size
	}
}

func videoFileRank(f *MDBFile) int64 {
	rank, ok := videoSizeRankMap[f.VideoSize.String]
	if !ok {
		rank = 0
	}
	if val, ok := videoMimeTypeRankMap[f.MimeType.String]; ok {
		rank += val * 10
	}
	return rank
}

func audioFileRank(f *MDBFile) int64 {
	rank := f.Size
	if val, ok := audioMimeTypeRankMap[f.MimeType.String]; ok {
		rank += 1<<(64-val) - 1
	}
	return rank
}
