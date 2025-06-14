package shape

type DataType string

const (
	JSONB   DataType = "jsonb"
	Any     DataType = "any"
	Bool    DataType = "bool"
	Number  DataType = "number"
	Integer DataType = "integer"
	Text    DataType = "text"
)

type Column struct {
	Name string   `json:"name" validate:"required,alphanum,min=1,max=255"`
	Type DataType `json:"type" validate:"required,oneof=jsonb any bool number integer text"`
	Path string   `json:"path" validate:"required,min=1,max=255"`
}

type Index struct {
	Columns []string `json:"columns" validate:"required,min=1,dive,alphanum,min=1,max=255"`
}

type Filter struct {
	Path  string `json:"path" validate:"required,min=1,max=255"`
	Value string `json:"value" validate:"required,min=1,max=255"`
}

type Shape struct {
	Columns []Column `json:"columns" validate:"required,min=1,dive"`
	Indexes []Index  `json:"indexes" validate:"omitempty,dive"`
	Filters []Filter `json:"filter,omitempty"`
}
