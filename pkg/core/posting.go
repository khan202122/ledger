package core

import (
	"database/sql/driver"
	"encoding/json"
	"regexp"
)

type Posting struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	Asset       string `json:"asset"`
}

type Postings []Posting

func (p Postings) Reverse() {
	if len(p) == 1 {
		p[0].Source, p[0].Destination = p[0].Destination, p[0].Source
		return
	}
	for i := len(p)/2 - 1; i >= 0; i-- {
		opp := len(p) - 1 - i
		p[i], p[opp] = p[opp], p[i]
		p[i].Source, p[i].Destination = p[i].Destination, p[i].Source
		p[opp].Source, p[opp].Destination = p[opp].Destination, p[opp].Source
	}
}

// Scan - Implement the database/sql scanner interface
func (p *Postings) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	*p = Postings{}
	switch vv := v.(type) {
	case []uint8:
		return json.Unmarshal(vv, p)
	case string:
		return json.Unmarshal([]byte(vv), p)
	default:
		panic("not supported type")
	}
}

var addressRegexp = regexp.MustCompile("^[a-zA-Z_0-9]+(:[a-zA-Z_0-9]+){0,}$")

func ValidateAddress(addr string) bool {
	return addressRegexp.Match([]byte(addr))
}
