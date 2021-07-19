package pqm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func InitTable(tx *sql.Tx, table *Table) error {
	t, err := scanInfo(table.Title, tx)
	if err != nil {
		return fmt.Errorf("scan table %v is failed: %v", table.Title, err)
	}
	qry := `create table if not exists ` + table.Title + ` (id bigserial primary Key);`

	for _, v := range table.Column {
		v := v.Get()
		if tt, ok := t.Column[v.Title]; ok {
			tt := tt.Get()
			if tt.Type != v.Type {
				deleteColumn(&qry, table.Title, v.Title)
				addColumn(&qry, table.Title, v)
				continue
			}
			if tt.Type == "character varying" && tt.Length != v.Length {
				setLengthColumn(&qry, table.Title, v.Title, v.Type, v.Length)
			}
			if tt.IsNotNull != v.IsNotNull {
				setNullColumn(&qry, table.Title, v.Title, v.IsNotNull)
			}
			if tt.Default != buildDef(v.Default, v.Type) {
				fmt.Println(tt.Default, buildDef(v.Default, v.Type))
				setDefaultColumn(&qry, table.Title, v.Title, v.Type, v.Default)
			}
		} else {
			addColumn(&qry, table.Title, v)
		}
	}

	keys := ""
	for _, v := range table.Keys {
		v := v.Get()
		if kk, ok := t.Keys[v.Title]; ok {
			kk := kk.Get()
			if kk.IsReferences != v.IsReferences ||
				kk.IsUnicue != v.IsUnicue ||
				(v.ToTableTitle != "" && kk.ToTableTitle != v.ToTableTitle) ||
				!equalsArray(v.FromColumns, kk.FromColumns) ||
				!equalsArray(v.ToColumns, kk.ToColumns) {
				deleteKey(&keys, table.Title, v.Title)
				addKey(&keys, table.Title, v)
			}
			delete(t.Keys, v.Title)
		} else {
			addKey(&keys, table.Title, v)
		}
	}
	for k := range t.Keys {
		deleteKey(&qry, table.Title, k)
	}
	qry += keys

	fmt.Println(qry)

	if _, err = tx.Exec(qry); err != nil {
		return fmt.Errorf("migration is failed: %v", err)
	}
	return nil
}

func Integer(title string, def int32, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "integer",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func Bigint(title string, def int64, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "bigint",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func DPrecision(title string, def float64, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "double precision",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func VarChar(title, def string, length int64, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "character varying",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    length,
	}
}
func Text(title, def string, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "text",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func Boolean(title string, def bool) Column {
	return &column{
		Title:   title,
		Type:    "boolean",
		Default: def,
	}
}
func Bytea(title string, def []byte, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "bytea",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func Array(title string, def []interface{}, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "array",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func JsonB(title string, def json.RawMessage, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "jsonb",
		Default:   def,
		IsNotNull: isNotNull,
		Length:    0,
	}
}
func Timestamp(title string, def time.Time, isNotNull bool) Column {
	return &column{
		Title:     title,
		Type:      "timestamp without time zone",
		Default:   def,
		IsNotNull: isNotNull,
	}
}

func Unique(title string, fromColumn []string) Key {
	return &key{
		Title:       title,
		FromColumns: fromColumn,
		IsUnicue:    true,
	}
}
func Reference(title string, fromColumn, toTable, toColumn string) Key {
	return &key{
		Title:        title,
		FromColumns:  []string{fromColumn},
		ToColumns:    []string{toColumn},
		ToTableTitle: toTable,
		IsReferences: true,
	}
}

func scanInfo(title string, tx *sql.Tx) (*table, error) {
	t := &table{
		Title:  title,
		Column: map[string]Column{},
		Keys:   map[string]Key{},
	}
	res, err := tx.Query(`
	select
		c.column_name,
		c.data_type,
		case when c.column_default is not null then c.column_default else '' end,
		case when c.character_maximum_length is not null then c.character_maximum_length else 0 end,
		c.is_nullable,
		case when kcu.constraint_name is not null then kcu.constraint_name else '' end,
		case when tc.constraint_type is not null then tc.constraint_type else '' end,
		case when ccu.column_name is not null then ccu.column_name else '' end,
		case when ccu.table_name is not null then ccu.table_name else '' end
	from
		information_schema."columns" c
	left join information_schema.key_column_usage kcu on
		kcu.column_name = c.column_name
		and
		kcu.table_name = c.table_name 
	left join information_schema.constraint_column_usage ccu on
		ccu.constraint_name = kcu.constraint_name
	left join information_schema.table_constraints tc on
		tc.constraint_name = ccu.constraint_name
	where
		c.table_name = $1
		and c.column_name <> 'id'
	`, title)
	if err != nil {
		return t, fmt.Errorf("table info not found: %v", err)
	}
	defer res.Close()

	for res.Next() {
		ti := &tableInfo{}
		if err = res.Scan(&ti.Column, &ti.ColumnType, &ti.Default, &ti.Length, &ti.IsNotNull, &ti.Key, &ti.KeyType, &ti.KeyColumn, &ti.KeyTable); err != nil {
			return t, fmt.Errorf("Table scan is failed: %v", err)
		}
		if _, ok := t.Column[ti.Column]; !ok {
			t.Column[ti.Column] = &column{
				Type:    ti.ColumnType,
				Default: ti.Default,
				Length:  ti.Length,
			}
			if ti.IsNotNull == "NO" {
				t.Column[ti.Column].Get().IsNotNull = true
			}
		}
		if ti.Key != "" {
			if k, ok := t.Keys[ti.Key]; ok {
				k := k.Get()
			fColumns:
				for {
					for _, c := range k.FromColumns {
						if c == ti.Column {
							break fColumns
						}
					}
					k.FromColumns = append(k.FromColumns, ti.Column)
					break
				}
				if ti.KeyTable != title {
				tColumns:
					for {
						for _, c := range k.ToColumns {
							if c == ti.KeyColumn {
								break tColumns
							}
						}
						k.ToColumns = append(k.ToColumns, ti.KeyColumn)
						break
					}
				}
			} else {
				k := &key{
					FromColumns:  []string{ti.Column},
					ToColumns:    []string{},
					ToTableTitle: ti.KeyTable,
				}
				if ti.KeyTable != title {
					k.ToColumns = []string{ti.KeyColumn}
				}
				switch ti.KeyType {
				case "UNIQUE":
					k.IsUnicue = true
				case "FOREIGN KEY":
					k.IsReferences = true
				}
				t.Keys[ti.Key] = k
			}
		}
	}

	return t, nil
}

func addColumn(qry *string, title string, c *column) {
	*qry += fmt.Sprintf("\nalter table %v add %v %v", title, c.Title, c.Type)
	if c.Type == "character varying" && c.Length > 0 {
		*qry += fmt.Sprintf("(%v)", c.Length)
	}
	if v := buildDef(c.Default, c.Type); v != "" {
		*qry += fmt.Sprintf(" default %v", v)
	}
	if c.IsNotNull {
		*qry += " not null"
	}
	*qry += ";"
}
func setLengthColumn(qry *string, title, Key, typ string, length int64) {
	*qry += fmt.Sprintf("\nalter table %v alter Column %v type %v", title, Key, typ)
	if length > 0 {
		*qry += fmt.Sprintf("(%v)", length)
	}
	*qry += fmt.Sprintf(" using %v::%v;", Key, typ)
}
func setNullColumn(qry *string, title, Key string, isNotNull bool) {
	*qry += fmt.Sprintf("\nalter table %v alter Column %v", title, Key)
	if isNotNull {
		*qry += " set not null;"
	} else {
		*qry += " drop not null;"
	}
}
func setDefaultColumn(qry *string, title, Key, typ string, def interface{}) {
	*qry += fmt.Sprintf("\nalter table %v alter Column %v", title, Key)
	if v := buildDef(def, typ); v != "" {
		*qry += fmt.Sprintf(" set default %v;", v)
	} else {
		*qry += " drop default;"
	}
}
func deleteColumn(qry *string, title, Key string) {
	*qry += fmt.Sprintf("\nalter table %v drop Column %v;", title, Key)
}

func addKey(qry *string, title string, k *key) {
	if k.IsUnicue {
		if len(k.FromColumns) > 0 {
			*qry += fmt.Sprintf("\nalter table %v add constraint %v unique(", title, k.Title)
			*qry += strings.Join(k.FromColumns, ",")
			*qry += ");"
		}
	} else if k.IsReferences {
		if len(k.FromColumns) == 1 && len(k.ToColumns) == 1 {
			*qry += fmt.Sprintf("\nalter table %v add constraint %v foreign Key (%v) references %v(%v) on delete cascade;", title, k.Title, k.FromColumns[0], k.ToTableTitle, k.ToColumns[0])
		}
	}
}
func deleteKey(qry *string, title, Key string) {
	*qry += fmt.Sprintf("\nalter table %v drop constraint %v;", title, Key)
}

func buildDef(def interface{}, typ string) string {
	if def != nil {
		if v, ok := def.(string); ok {
			return fmt.Sprintf("'%v'::%v", v, typ)
		} else if v, ok := def.(json.RawMessage); ok {
			return fmt.Sprintf("'%v'::%v", string(v), typ)
		} else if v, ok := def.(time.Time); ok {
			return fmt.Sprintf("'%v'::%v", v.Format("2006-01-02 15:04:05"), typ)
		} else if v, ok := def.([]byte); ok {
			return fmt.Sprintf("'%v'::%v", string(v), typ)
		} else {
			return fmt.Sprintf("%v::%v", def, typ)
		}
	}
	return ""
}
