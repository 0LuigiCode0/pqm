package pqm

type Table struct {
	Title  string
	Column []Column
	Keys   []Key
}

type Column interface {
	Get() *column
}
type column struct {
	Title     string
	Type      string
	IsNotNull bool
	Default   interface{}
	Length    int64
}

type Key interface {
	Get() *key
}
type key struct {
	Title           string
	FromColumns     []string
	ToColumns       []string
	ToTableTitle    string
	IsUnicue        bool
	IsReferences    bool
	IsUpdateCascade bool
}

type table struct {
	Title  string
	Column map[string]Column
	Keys   map[string]Key
}
type tableInfo struct {
	Column     string
	ColumnType string
	Default    string
	Length     int64
	IsNotNull  string
	Key        string
	KeyType    string
	KeyColumn  string
	KeyTable   string
}
