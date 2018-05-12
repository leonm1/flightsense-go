package flight

import (
	"fmt"
	"strconv"
	"time"

	airlines "github.com/leonm1/airlines-go"
	airports "github.com/leonm1/airports-go"
)

// Flight includes data relating to weather conditions and general flight information
type Flight struct {
	Date                  string           `json:"fullDate" csv:"FL_DATE"`
	Carrier               airlines.Airline `json:"carrier" csv:"CARRIER"`
	Origin                airports.Airport `json:"origin" csv:"ORIGIN"`
	Destination           airports.Airport `json:"destination" csv:"DEST"`
	ScheduledDep          time.Time        `json:"scheduledDep" csv:"CRS_DEP_TIME"`
	ActualDep             time.Time        `json:"actualDep" csv:"DEP_TIME"`
	Delay                 int              `json:"delay" csv:"DEP_DELAY"`
	Cancelled             bool             `json:"cancelled" csv:"CANCELLED"`
	CancellationCode      string           `json:"cancellationCode" csv:"CANCELLATION_CODE"`
	Diverted              bool             `json:"diverted" csv:"DIVERTED"`
	DaylightSavings       string           `json:"dst" csv:"DST"`
	TempOrigin            float64          `json:"tempOrigin" csv:"TEMP_ORIG"`
	PrecipIntensityOrigin float64          `json:"originPrecipIntensity" csv:"PRECIP_ORIG"`
	PrecipTypeOrigin      string           `json:"originPrecipType" csv:"PRECIP_TYPE_ORIG"`
	TempDest              float64          `json:"destTemp" csv:"TEMP_DEST"`
	PrecipIntensityDest   float64          `json:"destPrecipIntensity" csv:"PRECIP_DEST"`
	PrecipTypeDest        string           `json:"destPrecipType" csv:"PRECIP_TYPE_DEST"`
}

var header = []string{
	"absoluteTime",
	"year",
	"month",
	"day",
	"airline",
	"originAirport",
	"destAirport",
	"scheduledDeparture",
	"actualDeparture",
	"delay",
	"cancelled",
	"cancellationCode",
	"diverted",
	"tempOrigin",
	"precipTypeOrigin",
	"precipIntensityOrigin",
	"tempDest",
	"precipTypeDest",
	"precipIntensityDest",
}

// Headers outputs a slice containing the field names of the struct
func (f *Flight) Headers() []string {
	return header
}

// ToSlice outputs a slice of values from
func (f *Flight) ToSlice() []string {
	ret := make([]string, 19)

	ret[0] = f.Date
	ret[1] = fmt.Sprint(f.ScheduledDep.Year())
	ret[2] = f.ScheduledDep.Month().String()
	ret[3] = fmt.Sprint(f.ScheduledDep.Day())
	ret[4] = f.Carrier.Name
	ret[5] = f.Origin.IATA
	ret[6] = f.Destination.IATA
	ret[7] = fmt.Sprintf("%02d%02d", f.ScheduledDep.Hour(), f.ScheduledDep.Minute())
	ret[8] = fmt.Sprintf("%02d%02d", f.ActualDep.Hour(), f.ActualDep.Minute())
	ret[9] = fmt.Sprint(f.Delay)
	ret[10] = strconv.FormatBool(f.Cancelled)
	ret[11] = f.CancellationCode
	ret[12] = strconv.FormatBool(f.Diverted)
	ret[13] = strconv.FormatFloat(f.TempOrigin, 'f', -1, 64)
	ret[14] = f.PrecipTypeOrigin
	ret[15] = strconv.FormatFloat(f.PrecipIntensityOrigin, 'f', -1, 64)
	ret[16] = strconv.FormatFloat(f.TempDest, 'f', -1, 64)
	ret[17] = f.PrecipTypeDest
	ret[18] = strconv.FormatFloat(f.PrecipIntensityDest, 'f', -1, 64)

	return ret
}
