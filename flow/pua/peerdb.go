package pua

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
	"github.com/yuin/gopher-lua"

	"github.com/PeerDB-io/glua64"
	"github.com/PeerDB-io/gluabit32"
	"github.com/PeerDB-io/gluajson"
	"github.com/PeerDB-io/gluamsgpack"
	"github.com/PeerDB-io/gluautf8"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
)

var (
	LuaRecord = glua64.UserDataType[model.Record[model.RecordItems]]{Name: "peerdb_record"}
	LuaRow    = glua64.UserDataType[model.RecordItems]{Name: "peerdb_row"}
)

func RegisterTypes(ls *lua.LState) {
	glua64.Loader(ls)
	ls.Env.RawSetString("loadfile", lua.LNil)
	ls.Env.RawSetString("dofile", lua.LNil)

	// gopher-lua provides 2 loaders {preload, file}
	// overwrite file loader with one retrieving scripts from database
	loaders := ls.G.Registry.RawGetString("_LOADERS").(*lua.LTable)
	loaders.RawSetInt(2, ls.NewFunction(LoadPeerdbScript))

	ls.PreloadModule("bit32", gluabit32.Loader)
	ls.PreloadModule("json", gluajson.Loader)
	ls.PreloadModule("msgpack", gluamsgpack.Loader)
	ls.PreloadModule("utf8", gluautf8.Loader)

	mt := ls.NewTypeMetatable("Array")
	mt.RawSetString("__json", ls.NewFunction(LuaArrayJson))
	mt.RawSetString("__msgpack", ls.NewFunction(LuaArrayMsgpack))

	mt = LuaRecord.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRecordIndex))
	mt.RawSetString("__json", ls.NewFunction(LuaRecordJson))

	mt = LuaRow.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaRowIndex))
	mt.RawSetString("__len", ls.NewFunction(LuaRowLen))

	mt = shared.LuaUuid.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaUuidIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaUuidString))
	mt.RawSetString("__eq", ls.NewFunction(LuaUuidEq))
	mt.RawSetString("__msgpack", ls.NewFunction(LuaUuidMsgpack))

	mt = shared.LuaTime.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaTimeIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaTimeString))
	mt.RawSetString("__eq", ls.NewFunction(LuaTimeEq))
	mt.RawSetString("__le", ls.NewFunction(LuaTimeLe))
	mt.RawSetString("__lt", ls.NewFunction(LuaTimeLt))

	mt = shared.LuaBigInt.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaBigIntIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaBigIntString))
	mt.RawSetString("__eq", ls.NewFunction(LuaBigIntEq))
	mt.RawSetString("__le", ls.NewFunction(LuaBigIntLe))
	mt.RawSetString("__lt", ls.NewFunction(LuaBigIntLt))
	mt.RawSetString("__unm", ls.NewFunction(LuaBigIntUnm))

	mt = shared.LuaDecimal.NewMetatable(ls)
	mt.RawSetString("__index", ls.NewFunction(LuaDecimalIndex))
	mt.RawSetString("__tostring", ls.NewFunction(LuaDecimalString))
	mt.RawSetString("__eq", ls.NewFunction(LuaDecimalEq))
	mt.RawSetString("__le", ls.NewFunction(LuaDecimalLe))
	mt.RawSetString("__lt", ls.NewFunction(LuaDecimalLt))
	mt.RawSetString("__unm", ls.NewFunction(LuaDecimalUnm))
	mt.RawSetString("__add", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Add(d2)
	})))
	mt.RawSetString("__sub", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Sub(d2)
	})))
	mt.RawSetString("__mul", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Mul(d2)
	})))
	mt.RawSetString("__div", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Div(d2)
	})))
	mt.RawSetString("__mod", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Mod(d2)
	})))
	mt.RawSetString("__pow", ls.NewFunction(decimalBinop(func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal {
		return d1.Pow(d2)
	})))
	mt.RawSetString("__msgpack", ls.NewFunction(LuaDecimalString))

	peerdb := ls.NewTable()
	peerdb.RawSetString("RowTable", ls.NewFunction(LuaRowTable))
	peerdb.RawSetString("RowColumns", ls.NewFunction(LuaRowColumns))
	peerdb.RawSetString("RowColumnKind", ls.NewFunction(LuaRowColumnKind))
	peerdb.RawSetString("Now", ls.NewFunction(LuaNow))
	peerdb.RawSetString("UUID", ls.NewFunction(LuaUUID))
	peerdb.RawSetString("Decimal", ls.NewFunction(LuaParseDecimal))
	peerdb.RawSetString("type", ls.NewFunction(LuaType))
	peerdb.RawSetString("tostring", ls.NewFunction(LuaToString))
	ls.Env.RawSetString("peerdb", peerdb)
}

func LoadPeerdbScript(ls *lua.LState) int {
	ctx := ls.Context()
	name := ls.CheckString(1)
	pool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		ls.RaiseError("Connection failed loading %s: %s", name, err.Error())
		return 0
	}

	var source []byte
	err = pool.QueryRow(ctx, "select source from scripts where lang = 'lua' and name = $1", name).Scan(&source)
	if err != nil {
		if err == pgx.ErrNoRows {
			ls.Push(lua.LString("Could not find script " + name))
			return 1
		}
		ls.RaiseError("Failed to load script %s: %s", name, err.Error())
		return 0
	}

	fn, err := ls.Load(bytes.NewReader(source), name)
	if err != nil {
		ls.RaiseError(err.Error())
	}
	ls.Push(fn)
	return 1
}

func GetRowQ(ls *lua.LState, row model.RecordItems, col string) qvalue.QValue {
	qv, err := row.GetValueByColName(col)
	if err != nil {
		ls.RaiseError(err.Error())
		return nil
	}
	return qv
}

func LuaRowIndex(ls *lua.LState) int {
	row, key := LuaRow.StartIndex(ls)
	ls.Push(GetRowQ(ls, row, key).LValue(ls))
	return 1
}

func LuaRowLen(ls *lua.LState) int {
	row := LuaRow.StartMethod(ls)
	ls.Push(lua.LNumber(len(row.ColToVal)))
	return 1
}

func LuaRowTable(ls *lua.LState) int {
	row := LuaRow.StartMethod(ls)
	tbl := ls.CreateTable(0, len(row.ColToVal))
	for col, val := range row.ColToVal {
		tbl.RawSetString(col, val.LValue(ls))
	}
	ls.Push(tbl)
	return 1
}

func LuaRowColumns(ls *lua.LState) int {
	row := LuaRow.StartMethod(ls)
	tbl := ls.CreateTable(len(row.ColToVal), 0)
	idx := 0
	for col := range row.ColToVal {
		idx += 1
		tbl.RawSetInt(idx, lua.LString(col))
	}
	ls.Push(tbl)
	return 1
}

func LuaRowColumnKind(ls *lua.LState) int {
	row, key := LuaRow.StartIndex(ls)
	ls.Push(lua.LString(GetRowQ(ls, row, key).Kind()))
	return 1
}

func LuaArrayJson(ls *lua.LState) int {
	ls.Push(&lua.LUserData{
		Value:     gluajson.Array(ls.CheckTable(1)),
		Env:       ls.Env,
		Metatable: nil,
	})
	return 1
}

func LuaArrayMsgpack(ls *lua.LState) int {
	ls.Push(&lua.LUserData{
		Value:     gluamsgpack.Array(ls.CheckTable(1)),
		Env:       ls.Env,
		Metatable: nil,
	})
	return 1
}

func LuaRecordIndex(ls *lua.LState) int {
	record, key := LuaRecord.StartIndex(ls)
	switch key {
	case "kind":
		switch record.(type) {
		case *model.InsertRecord[model.RecordItems]:
			ls.Push(lua.LString("insert"))
		case *model.UpdateRecord[model.RecordItems]:
			ls.Push(lua.LString("update"))
		case *model.DeleteRecord[model.RecordItems]:
			ls.Push(lua.LString("delete"))
		case *model.RelationRecord[model.RecordItems]:
			ls.Push(lua.LString("relation"))
		}
	case "row":
		items := record.GetItems()
		if items.ColToVal != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "old":
		var items model.RecordItems
		switch rec := record.(type) {
		case *model.UpdateRecord[model.RecordItems]:
			items = rec.OldItems
		case *model.DeleteRecord[model.RecordItems]:
			items = rec.Items
		}
		if items.ColToVal != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "new":
		var items model.RecordItems
		switch rec := record.(type) {
		case *model.InsertRecord[model.RecordItems]:
			items = rec.Items
		case *model.UpdateRecord[model.RecordItems]:
			items = rec.NewItems
		}
		if items.ColToVal != nil {
			ls.Push(LuaRow.New(ls, items))
		} else {
			ls.Push(lua.LNil)
		}
	case "checkpoint":
		ls.Push(glua64.I64.New(ls, record.GetCheckpointID()))
	case "commit_time":
		ls.Push(shared.LuaTime.New(ls, record.GetCommitTime()))
	case "target":
		ls.Push(lua.LString(record.GetDestinationTableName()))
	case "source":
		ls.Push(lua.LString(record.GetSourceTableName()))
	case "unchanged_columns":
		if ur, ok := record.(*model.UpdateRecord[model.RecordItems]); ok {
			tbl := ls.CreateTable(0, len(ur.UnchangedToastColumns))
			for col := range ur.UnchangedToastColumns {
				tbl.RawSetString(col, lua.LTrue)
			}
			ls.Push(tbl)
		} else {
			ls.Push(lua.LNil)
		}
	default:
		return 0
	}
	return 1
}

func LuaRecordJson(ls *lua.LState) int {
	ud := ls.CheckUserData(1)
	tbl := ls.CreateTable(0, 7)
	for _, key := range []string{
		"kind", "old", "new", "checkpoint", "commit_time", "source",
	} {
		tbl.RawSetString(key, ls.GetField(ud, key))
	}
	if ur, ok := ud.Value.(*model.UpdateRecord[model.RecordItems]); ok {
		if len(ur.UnchangedToastColumns) > 0 {
			unchanged := ls.CreateTable(len(ur.UnchangedToastColumns), 0)
			for col := range ur.UnchangedToastColumns {
				unchanged.Append(lua.LString(col))
			}
			tbl.RawSetString("unchanged_columns", unchanged)
		}
	}
	ls.Push(tbl)
	return 1
}

func LuaUuidIndex(ls *lua.LState) int {
	_, val := shared.LuaUuid.Check(ls, 1)
	key := ls.CheckNumber(2)
	ki := int(key)
	if ki >= 0 && ki < 16 {
		ls.Push(lua.LNumber(val[ki]))
		return 1
	}
	return 0
}

func LuaUuidString(ls *lua.LState) int {
	val := shared.LuaUuid.StartMethod(ls)
	ls.Push(lua.LString(val.String()))
	return 1
}

func LuaUuidEq(ls *lua.LState) int {
	val1 := shared.LuaUuid.StartMethod(ls)
	_, val2 := shared.LuaUuid.Check(ls, 2)
	ls.Push(lua.LBool(val1 == val2))
	return 1
}

func LuaUuidMsgpack(ls *lua.LState) int {
	val := shared.LuaUuid.StartMethod(ls)
	ls.Push(&lua.LUserData{
		Value:     gluamsgpack.Bin(val[:]),
		Env:       ls.Env,
		Metatable: nil,
	})
	return 1
}

func LuaNow(ls *lua.LState) int {
	ls.Push(shared.LuaTime.New(ls, time.Now()))
	return 1
}

func LuaUUID(ls *lua.LState) int {
	if ls.GetTop() == 0 {
		ls.Push(shared.LuaUuid.New(ls, uuid.New()))
	} else if v, ok := ls.Get(1).(lua.LString); ok {
		ls.Push(shared.LuaUuid.New(ls, uuid.MustParse(string(v))))
	} else {
		ls.RaiseError("uuid must be created from string")
	}
	return 1
}

func LVAsDecimal(ls *lua.LState, lv lua.LValue) decimal.Decimal {
	switch v := lv.(type) {
	case lua.LNumber:
		return decimal.NewFromFloat(float64(v))
	case lua.LString:
		d, err := decimal.NewFromString(string(v))
		if err != nil {
			ls.RaiseError(err.Error())
		}
		return d
	case *lua.LUserData:
		switch v := v.Value.(type) {
		case int64:
			return decimal.NewFromInt(v)
		case uint64:
			return decimal.NewFromUint64(v)
		case *big.Int:
			return decimal.NewFromBigInt(v, 0)
		case decimal.Decimal:
			return v
		default:
			ls.RaiseError("cannot create decimal from %T", v)
		}
	default:
		ls.RaiseError("cannot create decimal from %s", v.Type())
	}
	return decimal.Decimal{}
}

func LuaParseDecimal(ls *lua.LState) int {
	ls.Push(shared.LuaDecimal.New(ls, LVAsDecimal(ls, ls.Get(1))))
	return 1
}

func LuaType(ls *lua.LState) int {
	val := ls.Get(1)
	if ud, ok := val.(*lua.LUserData); ok {
		ls.Push(lua.LString(fmt.Sprintf("%T", ud.Value)))
		return 1
	}
	return 0
}

func LuaToString(ls *lua.LState) int {
	val := ls.Get(1)
	if ud, ok := val.(*lua.LUserData); ok {
		ls.Push(lua.LString(fmt.Sprint(ud.Value)))
		return 1
	}
	return 0
}

func LuaTimeIndex(ls *lua.LState) int {
	tm, key := shared.LuaTime.StartIndex(ls)
	switch key {
	case "unix_nano":
		ls.Push(glua64.I64.New(ls, tm.UnixNano()))
	case "unix_micro":
		ls.Push(glua64.I64.New(ls, tm.UnixMicro()))
	case "unix_milli":
		ls.Push(glua64.I64.New(ls, tm.UnixMilli()))
	case "unix_second":
		ls.Push(glua64.I64.New(ls, tm.Unix()))
	case "unix":
		ls.Push(lua.LNumber(float64(tm.Unix()) + float64(tm.Nanosecond())/1e9))
	case "year":
		ls.Push(lua.LNumber(tm.Year()))
	case "month":
		ls.Push(lua.LNumber(tm.Month()))
	case "day":
		ls.Push(lua.LNumber(tm.Day()))
	case "yearday":
		ls.Push(lua.LNumber(tm.YearDay()))
	case "hour":
		ls.Push(lua.LNumber(tm.Hour()))
	case "minute":
		ls.Push(lua.LNumber(tm.Minute()))
	case "second":
		ls.Push(lua.LNumber(tm.Second()))
	case "nanosecond":
		ls.Push(lua.LNumber(tm.Nanosecond()))
	default:
		return 0
	}
	return 1
}

func LuaTimeString(ls *lua.LState) int {
	tm := shared.LuaTime.StartMethod(ls)
	ls.Push(lua.LString(tm.String()))
	return 1
}

func LuaTimeEq(ls *lua.LState) int {
	t1 := shared.LuaTime.StartMethod(ls)
	_, t2 := shared.LuaTime.Check(ls, 2)
	ls.Push(lua.LBool(t1.Compare(t2) == 0))
	return 1
}

func LuaTimeLe(ls *lua.LState) int {
	t1 := shared.LuaTime.StartMethod(ls)
	_, t2 := shared.LuaTime.Check(ls, 2)
	ls.Push(lua.LBool(t1.Compare(t2) <= 0))
	return 1
}

func LuaTimeLt(ls *lua.LState) int {
	t1 := shared.LuaTime.StartMethod(ls)
	_, t2 := shared.LuaTime.Check(ls, 2)
	ls.Push(lua.LBool(t1.Compare(t2) == -1))
	return 1
}

func LuaBigIntIndex(ls *lua.LState) int {
	bi, key := shared.LuaBigInt.StartIndex(ls)
	switch key {
	case "sign":
		ls.Push(lua.LNumber(bi.Sign()))
	case "bytes":
		ls.Push(lua.LString(bi.Bytes()))
	case "int64":
		ls.Push(glua64.I64.New(ls, bi.Int64()))
	case "is64":
		ls.Push(lua.LBool(bi.IsInt64()))
	default:
		return 0
	}
	return 1
}

func LuaBigIntString(ls *lua.LState) int {
	bi := shared.LuaBigInt.StartMethod(ls)
	ls.Push(lua.LString(bi.String()))
	return 1
}

func LuaBigIntUnm(ls *lua.LState) int {
	bi := shared.LuaBigInt.StartMethod(ls)
	ls.Push(shared.LuaBigInt.New(ls, new(big.Int).Neg(bi)))
	return 1
}

func LuaBigIntEq(ls *lua.LState) int {
	t1 := shared.LuaBigInt.StartMethod(ls)
	_, t2 := shared.LuaBigInt.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) == 0))
	return 1
}

func LuaBigIntLe(ls *lua.LState) int {
	t1 := shared.LuaBigInt.StartMethod(ls)
	_, t2 := shared.LuaBigInt.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) <= 0))
	return 1
}

func LuaBigIntLt(ls *lua.LState) int {
	t1 := shared.LuaBigInt.StartMethod(ls)
	_, t2 := shared.LuaBigInt.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) == -1))
	return 1
}

func LuaDecimalIndex(ls *lua.LState) int {
	num, key := shared.LuaDecimal.StartIndex(ls)
	switch key {
	case "coefficient":
		ls.Push(shared.LuaBigInt.New(ls, num.Coefficient()))
	case "coefficient64":
		ls.Push(glua64.I64.New(ls, num.CoefficientInt64()))
	case "exponent":
		ls.Push(lua.LNumber(num.Exponent()))
	case "bigint":
		ls.Push(shared.LuaBigInt.New(ls, num.BigInt()))
	case "int64":
		ls.Push(glua64.I64.New(ls, num.IntPart()))
	case "float64":
		ls.Push(lua.LNumber(num.InexactFloat64()))
	default:
		return 0
	}
	return 1
}

func LuaDecimalString(ls *lua.LState) int {
	num := shared.LuaDecimal.StartMethod(ls)
	ls.Push(lua.LString(num.String()))
	return 1
}

func LuaDecimalUnm(ls *lua.LState) int {
	num := shared.LuaDecimal.StartMethod(ls)
	ls.Push(shared.LuaDecimal.New(ls, num.Neg()))
	return 1
}

func decimalBinop(f func(d1 decimal.Decimal, d2 decimal.Decimal) decimal.Decimal) func(ls *lua.LState) int {
	return func(ls *lua.LState) int {
		ls.Push(shared.LuaDecimal.New(ls, f(LVAsDecimal(ls, ls.Get(1)), LVAsDecimal(ls, ls.Get(2)))))
		return 1
	}
}

func LuaDecimalEq(ls *lua.LState) int {
	t1 := shared.LuaDecimal.StartMethod(ls)
	_, t2 := shared.LuaDecimal.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) == 0))
	return 1
}

func LuaDecimalLe(ls *lua.LState) int {
	t1 := shared.LuaDecimal.StartMethod(ls)
	_, t2 := shared.LuaDecimal.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) <= 0))
	return 1
}

func LuaDecimalLt(ls *lua.LState) int {
	t1 := shared.LuaDecimal.StartMethod(ls)
	_, t2 := shared.LuaDecimal.Check(ls, 2)
	ls.Push(lua.LBool(t1.Cmp(t2) == -1))
	return 1
}
