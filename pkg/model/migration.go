package model

import "time"

type Migration struct {
	Version   int       `ion:"version"`
	UpdatedAt time.Time `ion:"updatedAt"`
	Active    bool      `ion:"active"`
}
