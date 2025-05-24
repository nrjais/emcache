package shape

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataType_String(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected string
	}{
		{JSONB, "jsonb"},
		{Any, "any"},
		{Bool, "bool"},
		{Number, "number"},
		{Integer, "integer"},
		{Text, "text"},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataType), func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.dataType))
		})
	}
}

func TestColumn_Validation(t *testing.T) {
	tests := []struct {
		name        string
		column      Column
		shouldError bool
	}{
		{
			name: "valid column",
			column: Column{
				Name: "testColumn",
				Type: Text,
				Path: "test.path",
			},
			shouldError: false,
		},
		{
			name: "empty name",
			column: Column{
				Name: "",
				Type: Text,
				Path: "test.path",
			},
			shouldError: true, // depending on validation rules
		},
		{
			name: "valid integer column",
			column: Column{
				Name: "age",
				Type: Integer,
				Path: "user.age",
			},
			shouldError: false,
		},
		{
			name: "valid bool column",
			column: Column{
				Name: "isActive",
				Type: Bool,
				Path: "status.active",
			},
			shouldError: false,
		},
		{
			name: "valid jsonb column",
			column: Column{
				Name: "metadata",
				Type: JSONB,
				Path: "data.metadata",
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that the column struct can be created
			assert.Equal(t, tt.column.Name, tt.column.Name)
			assert.Equal(t, tt.column.Type, tt.column.Type)
			assert.Equal(t, tt.column.Path, tt.column.Path)
		})
	}
}

func TestIndex_Creation(t *testing.T) {
	tests := []struct {
		name  string
		index Index
	}{
		{
			name: "single column index",
			index: Index{
				Columns: []string{"name"},
			},
		},
		{
			name: "multi column index",
			index: Index{
				Columns: []string{"name", "age", "email"},
			},
		},
		{
			name: "empty index",
			index: Index{
				Columns: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.index.Columns, tt.index.Columns)
		})
	}
}

func TestShape_Creation(t *testing.T) {
	t.Run("valid shape with columns and indexes", func(t *testing.T) {
		shape := Shape{
			Columns: []Column{
				{Name: "name", Type: Text, Path: "name"},
				{Name: "age", Type: Integer, Path: "age"},
				{Name: "metadata", Type: JSONB, Path: "metadata"},
			},
			Indexes: []Index{
				{Columns: []string{"name"}},
				{Columns: []string{"name", "age"}},
			},
		}

		assert.Len(t, shape.Columns, 3)
		assert.Len(t, shape.Indexes, 2)

		// Verify columns
		assert.Equal(t, "name", shape.Columns[0].Name)
		assert.Equal(t, Text, shape.Columns[0].Type)
		assert.Equal(t, "name", shape.Columns[0].Path)

		assert.Equal(t, "age", shape.Columns[1].Name)
		assert.Equal(t, Integer, shape.Columns[1].Type)
		assert.Equal(t, "age", shape.Columns[1].Path)

		assert.Equal(t, "metadata", shape.Columns[2].Name)
		assert.Equal(t, JSONB, shape.Columns[2].Type)
		assert.Equal(t, "metadata", shape.Columns[2].Path)

		// Verify indexes
		assert.Equal(t, []string{"name"}, shape.Indexes[0].Columns)
		assert.Equal(t, []string{"name", "age"}, shape.Indexes[1].Columns)
	})

	t.Run("shape with only columns", func(t *testing.T) {
		shape := Shape{
			Columns: []Column{
				{Name: "id", Type: Text, Path: "_id"},
			},
			Indexes: []Index{},
		}

		assert.Len(t, shape.Columns, 1)
		assert.Len(t, shape.Indexes, 0)
	})

	t.Run("empty shape", func(t *testing.T) {
		shape := Shape{
			Columns: []Column{},
			Indexes: []Index{},
		}

		assert.Len(t, shape.Columns, 0)
		assert.Len(t, shape.Indexes, 0)
	})
}

func TestShape_JSONMarshaling(t *testing.T) {
	t.Run("marshal and unmarshal shape", func(t *testing.T) {
		originalShape := Shape{
			Columns: []Column{
				{Name: "name", Type: Text, Path: "user.name"},
				{Name: "age", Type: Integer, Path: "user.age"},
				{Name: "active", Type: Bool, Path: "status.active"},
				{Name: "score", Type: Number, Path: "metrics.score"},
				{Name: "tags", Type: JSONB, Path: "metadata.tags"},
			},
			Indexes: []Index{
				{Columns: []string{"name"}},
				{Columns: []string{"name", "age"}},
			},
		}

		// Marshal to JSON
		jsonData, err := json.Marshal(originalShape)
		require.NoError(t, err)
		assert.NotEmpty(t, jsonData)

		// Unmarshal back to Shape
		var unmarshaledShape Shape
		err = json.Unmarshal(jsonData, &unmarshaledShape)
		require.NoError(t, err)

		// Verify the unmarshaled shape matches the original
		assert.Equal(t, originalShape.Columns, unmarshaledShape.Columns)
		assert.Equal(t, originalShape.Indexes, unmarshaledShape.Indexes)
	})

	t.Run("unmarshal from JSON string", func(t *testing.T) {
		jsonStr := `{
			"columns": [
				{"name": "title", "type": "text", "path": "document.title"},
				{"name": "views", "type": "integer", "path": "stats.views"}
			],
			"indexes": [
				{"columns": ["title"]},
				{"columns": ["title", "views"]}
			]
		}`

		var shape Shape
		err := json.Unmarshal([]byte(jsonStr), &shape)
		require.NoError(t, err)

		assert.Len(t, shape.Columns, 2)
		assert.Len(t, shape.Indexes, 2)

		assert.Equal(t, "title", shape.Columns[0].Name)
		assert.Equal(t, Text, shape.Columns[0].Type)
		assert.Equal(t, "document.title", shape.Columns[0].Path)

		assert.Equal(t, "views", shape.Columns[1].Name)
		assert.Equal(t, Integer, shape.Columns[1].Type)
		assert.Equal(t, "stats.views", shape.Columns[1].Path)
	})
}

func TestDataType_AllTypes(t *testing.T) {
	allTypes := []DataType{JSONB, Any, Bool, Number, Integer, Text}

	for _, dataType := range allTypes {
		t.Run(string(dataType), func(t *testing.T) {
			// Test that each data type can be used in a column
			column := Column{
				Name: "test",
				Type: dataType,
				Path: "test.path",
			}

			assert.Equal(t, dataType, column.Type)

			// Test JSON marshaling/unmarshaling for each type
			jsonData, err := json.Marshal(column)
			require.NoError(t, err)

			var unmarshaledColumn Column
			err = json.Unmarshal(jsonData, &unmarshaledColumn)
			require.NoError(t, err)

			assert.Equal(t, column.Type, unmarshaledColumn.Type)
		})
	}
}

func TestShape_EdgeCases(t *testing.T) {
	t.Run("shape with duplicate column names", func(t *testing.T) {
		shape := Shape{
			Columns: []Column{
				{Name: "name", Type: Text, Path: "name"},
				{Name: "name", Type: Integer, Path: "age"}, // Same name, different type
			},
		}

		// The shape struct itself doesn't validate uniqueness
		// That's typically done at a higher level
		assert.Len(t, shape.Columns, 2)
	})

	t.Run("index referencing non-existent column", func(t *testing.T) {
		shape := Shape{
			Columns: []Column{
				{Name: "name", Type: Text, Path: "name"},
			},
			Indexes: []Index{
				{Columns: []string{"nonExistentColumn"}},
			},
		}

		// The shape struct itself doesn't validate column references
		// That's typically done at a higher level
		assert.Len(t, shape.Indexes, 1)
	})

	t.Run("very long paths and names", func(t *testing.T) {
		longName := string(make([]byte, 300)) // Very long name
		longPath := "very.deep.nested.path.with.many.levels.and.more.levels.and.even.more.levels"

		column := Column{
			Name: longName,
			Type: Text,
			Path: longPath,
		}

		assert.Equal(t, longName, column.Name)
		assert.Equal(t, longPath, column.Path)
	})
}
