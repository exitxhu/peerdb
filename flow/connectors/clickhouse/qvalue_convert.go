package connclickhouse

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

var clickhouseTypeToQValueKindMap = map[string]qvalue.QValueKind{
	"INT":           qvalue.QValueKindInt32,
	"BIGINT":        qvalue.QValueKindInt64,
	"FLOAT":         qvalue.QValueKindFloat64,
	"DOUBLE":        qvalue.QValueKindFloat64,
	"REAL":          qvalue.QValueKindFloat64,
	"VARCHAR":       qvalue.QValueKindString,
	"CHAR":          qvalue.QValueKindString,
	"TEXT":          qvalue.QValueKindString,
	"BOOLEAN":       qvalue.QValueKindBoolean,
	"DATETIME":      qvalue.QValueKindTimestamp,
	"TIMESTAMP":     qvalue.QValueKindTimestamp,
	"TIMESTAMP_NTZ": qvalue.QValueKindTimestamp,
	"TIMESTAMP_TZ":  qvalue.QValueKindTimestampTZ,
	"TIME":          qvalue.QValueKindTime,
	"DATE":          qvalue.QValueKindDate,
	"BLOB":          qvalue.QValueKindBytes,
	"BYTEA":         qvalue.QValueKindBytes,
	"BINARY":        qvalue.QValueKindBytes,
	"FIXED":         qvalue.QValueKindNumeric,
	"NUMBER":        qvalue.QValueKindNumeric,
	"DECIMAL":       qvalue.QValueKindNumeric,
	"NUMERIC":       qvalue.QValueKindNumeric,
	"VARIANT":       qvalue.QValueKindJSON,
	"GEOMETRY":      qvalue.QValueKindGeometry,
	"GEOGRAPHY":     qvalue.QValueKindGeography,
}

func qValueKindToClickhouseType(colType qvalue.QValueKind) (string, error) {
	val, err := colType.ToDWHColumnType(qvalue.QDWHTypeClickhouse)
	if err != nil {
		return "", err
	}

	return val, err
}

func clickhouseTypeToQValueKind(name string) (qvalue.QValueKind, error) {
	if val, ok := clickhouseTypeToQValueKindMap[name]; ok {
		return val, nil
	}
	return "", fmt.Errorf("unsupported database type name: %s", name)
}