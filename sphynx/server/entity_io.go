// Helper methods to read and write entities.
package main

import (
	"bufio"
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"os"
	"reflect"
)

type Entity interface {
	typeName() string // This will help deserializing a serialized entity
}

type ParquetEntity interface { // Get objects for ParquetReader and ParquetWriter to figure out the schema.
	toOrderedRows() array.Record
	readFromOrdered(rec array.Record) error
	unorderedRow() interface{}
}

func (_ *Scalar) typeName() string {
	return "Scalar"
}
func (_ *VertexSet) typeName() string {
	return "VertexSet"
}
func (_ *EdgeBundle) typeName() string {
	return "EdgeBundle"
}
func (_ *DoubleAttribute) typeName() string {
	return "DoubleAttribute"
}
func (_ *StringAttribute) typeName() string {
	return "StringAttribute"
}
func (_ *LongAttribute) typeName() string {
	return "LongAttribute"
}
func (_ *DoubleTuple2Attribute) typeName() string {
	return "DoubleTuple2Attribute"
}
func (_ *DoubleVectorAttribute) typeName() string {
	return "DoubleVectorAttribute"
}

var vertexSetSchema = arrow.NewSchema(
	[]arrow.Field{
		arrow.Field{Name: "sparkId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

func (v *VertexSet) toOrderedRows() array.Record {
	b := array.NewInt64Builder(arrowAllocator)
	defer b.Release()
	b.AppendValues(v.MappingToUnordered, nil)
	col := b.NewInt64Array()
	defer col.Release()
	return array.NewRecord(vertexSetSchema, []array.Interface{col}, -1)
}
func (v *VertexSet) readFromOrdered(rec array.Record) error {
	data := rec.Column(0).(*array.Int64).Int64Values()
	// Make a copy because counting references is harder.
	v.MappingToUnordered = make([]int64, len(data))
	for i, d := range data {
		v.MappingToUnordered[i] = d
	}
	return nil
}

type UnorderedVertexRow struct {
	Id int64 `parquet:"name=id, type=INT64"`
}

func (_ *VertexSet) unorderedRow() interface{} {
	return new(UnorderedVertexRow)
}
func (v *VertexSet) toUnorderedRows() []interface{} {
	rows := make([]interface{}, len(v.MappingToUnordered))
	for i, v := range v.MappingToUnordered {
		rows[i] = UnorderedVertexRow{Id: v}
	}
	return rows
}

var edgeBundleSchema = arrow.NewSchema(
	[]arrow.Field{
		arrow.Field{Name: "src", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		arrow.Field{Name: "dst", Type: arrow.PrimitiveTypes.Uint32, Nullable: false},
		arrow.Field{Name: "sparkId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

func (eb *EdgeBundle) toOrderedRows() array.Record {
	b1 := array.NewUint32Builder(arrowAllocator)
	defer b1.Release()
	for _, i := range eb.Src {
		b1.Append(uint32(i))
	}
	src := b1.NewUint32Array()
	defer src.Release()
	b2 := array.NewUint32Builder(arrowAllocator)
	defer b2.Release()
	for _, i := range eb.Dst {
		b2.Append(uint32(i))
	}
	dst := b2.NewUint32Array()
	defer dst.Release()
	b3 := array.NewInt64Builder(arrowAllocator)
	defer b3.Release()
	b3.AppendValues(eb.EdgeMapping, nil)
	ids := b3.NewInt64Array()
	defer ids.Release()
	return array.NewRecord(edgeBundleSchema, []array.Interface{src, dst, ids}, -1)
}
func (eb *EdgeBundle) readFromOrdered(rec array.Record) error {
	src := rec.Column(0).(*array.Uint32).Uint32Values()
	dst := rec.Column(1).(*array.Uint32).Uint32Values()
	ids := rec.Column(2).(*array.Int64).Int64Values()
	// Make a copy because counting references is harder.
	eb.Src = make([]SphynxId, len(src))
	eb.Dst = make([]SphynxId, len(dst))
	eb.EdgeMapping = make([]int64, len(ids))
	for i, id := range ids {
		eb.Src[i] = SphynxId(src[i])
		eb.Dst[i] = SphynxId(dst[i])
		eb.EdgeMapping[i] = id
	}
	return nil
}

type UnorderedEdgeRow struct {
	Id  int64 `parquet:"name=id, type=INT64"`
	Src int64 `parquet:"name=src, type=INT64"`
	Dst int64 `parquet:"name=dst, type=INT64"`
}

func (_ *EdgeBundle) unorderedRow() interface{} {
	return new(UnorderedEdgeRow)
}
func (eb *EdgeBundle) toUnorderedRows(vs1 *VertexSet, vs2 *VertexSet) []interface{} {
	rows := make([]interface{}, len(eb.EdgeMapping))
	for sphynxId, sparkId := range eb.EdgeMapping {
		rows[sphynxId] = UnorderedEdgeRow{
			Id:  sparkId,
			Src: vs1.MappingToUnordered[eb.Src[sphynxId]],
			Dst: vs2.MappingToUnordered[eb.Dst[sphynxId]],
		}
	}
	return rows
}

func AttributeToUnorderedRows(attr ParquetEntity, vs *VertexSet) []interface{} {
	a := reflect.ValueOf(attr)
	values := a.Elem().FieldByName("Values")
	defined := a.Elem().FieldByName("Defined")
	rows := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), 0, 0)
	rowType := reflect.TypeOf(attr.unorderedRow()).Elem()
	valueIndex := fieldIndex(rowType, "Value")
	idIndex := fieldIndex(rowType, "Id")
	numValues := values.Len()
	row := reflect.New(rowType).Elem()
	for i := 0; i < numValues; i++ {
		if defined.Index(i).Bool() {
			sparkId := vs.MappingToUnordered[i]
			row.Field(valueIndex).Set(values.Index(i))
			row.Field(idIndex).Set(reflect.ValueOf(sparkId))
			rows = reflect.Append(rows, row)
		}
	}
	return rows.Interface().([]interface{})
}

func InitializeAttribute(attr reflect.Value, numVS int) {
	values := attr.Elem().FieldByName("Values")
	newValues := reflect.MakeSlice(values.Type(), numVS, numVS)
	values.Set(newValues)
	defined := attr.Elem().FieldByName("Defined")
	newDefined := reflect.MakeSlice(defined.Type(), numVS, numVS)
	defined.Set(newDefined)
}

func fieldIndex(t reflect.Type, name string) int {
	f, ok := t.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf("no %s field in %v", name, t))
	}
	if len(f.Index) != 1 {
		panic(fmt.Sprintf("field %v in %v is too complex", name, t))
	}
	return f.Index[0]
}

func (a *StringAttribute) readFromOrdered(rec array.Record) error {
	col := rec.Column(0).(*array.String)
	a.Values = make([]string, col.Len())
	a.Defined = make([]bool, col.Len())
	for i := 0; i < col.Len(); i++ {
		a.Values[i] = col.Value(i)
		a.Defined[i] = col.IsValid(i)
	}
	return nil
}
func (a *StringAttribute) toOrderedRows() array.Record {
	b := array.NewStringBuilder(arrowAllocator)
	defer b.Release()
	b.AppendValues(a.Values, a.Defined)
	values := b.NewStringArray()
	defer values.Release()
	return array.NewRecord(stringAttributeSchema, []array.Interface{values}, -1)
}

func (a *DoubleAttribute) readFromOrdered(rec array.Record) error {
	col := rec.Column(0).(*array.Float64)
	a.Values = make([]float64, col.Len())
	a.Defined = make([]bool, col.Len())
	for i, v := range col.Float64Values() {
		a.Values[i] = v
		a.Defined[i] = col.IsValid(i)
	}
	return nil
}
func (a *DoubleAttribute) toOrderedRows() array.Record {
	b := array.NewFloat64Builder(arrowAllocator)
	defer b.Release()
	b.AppendValues(a.Values, a.Defined)
	values := b.NewFloat64Array()
	defer values.Release()
	return array.NewRecord(doubleAttributeSchema, []array.Interface{values}, -1)
}

func (a *DoubleVectorAttribute) readFromOrdered(rec array.Record) error {
	// TODO
	return nil
}
func (a *DoubleVectorAttribute) toOrderedRows() array.Record {
	// TODO
	return array.NewRecord(doubleVectorAttributeSchema, []array.Interface{}, -1)
}

func (a *LongAttribute) readFromOrdered(rec array.Record) error {
	col := rec.Column(0).(*array.Int64)
	a.Values = make([]int64, col.Len())
	a.Defined = make([]bool, col.Len())
	for i, v := range col.Int64Values() {
		a.Values[i] = v
		a.Defined[i] = col.IsValid(i)
	}
	return nil
}
func (a *LongAttribute) toOrderedRows() array.Record {
	b := array.NewInt64Builder(arrowAllocator)
	defer b.Release()
	b.AppendValues(a.Values, a.Defined)
	values := b.NewInt64Array()
	defer values.Release()
	return array.NewRecord(longAttributeSchema, []array.Interface{values}, -1)
}

var stringAttributeSchema = arrow.NewSchema([]arrow.Field{
	arrow.Field{Name: "values", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
var doubleAttributeSchema = arrow.NewSchema([]arrow.Field{
	arrow.Field{Name: "values", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil)
var doubleVectorAttributeSchema = arrow.NewSchema([]arrow.Field{
	arrow.Field{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64),
		Nullable: true}}, nil)
var longAttributeSchema = arrow.NewSchema([]arrow.Field{
	arrow.Field{Name: "values", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

type UnorderedStringAttributeRow struct {
	Id    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}

type UnorderedLongAttributeRow struct {
	Id    int64 `parquet:"name=id, type=INT64"`
	Value int64 `parquet:"name=value, type=INT64"`
}

func (_ *StringAttribute) unorderedRow() interface{} {
	return new(UnorderedStringAttributeRow)
}

func (_ *LongAttribute) unorderedRow() interface{} {
	return new(UnorderedLongAttributeRow)
}

type UnorderedDoubleAttributeRow struct {
	Id    int64   `parquet:"name=id, type=INT64"`
	Value float64 `parquet:"name=value, type=DOUBLE"`
}

func (_ *DoubleAttribute) unorderedRow() interface{} {
	return new(UnorderedDoubleAttributeRow)
}

func (a *DoubleTuple2Attribute) toOrderedRows() array.Record {
	// TODO
	return array.NewRecord(doubleVectorAttributeSchema, []array.Interface{}, -1)
}

func (a *DoubleTuple2Attribute) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	// TODO
	return nil
}

type UnorderedDoubleTuple2AttributeRow struct {
	Id    int64                      `parquet:"name=id, type=INT64"`
	Value DoubleTuple2AttributeValue `parquet:"name=value"`
}

func (_ *DoubleTuple2Attribute) unorderedRow() interface{} {
	return new(UnorderedDoubleTuple2AttributeRow)
}

type UnorderedDoubleVectorAttributeRow struct {
	Id    int64                      `parquet:"name=id, type=INT64"`
	Value DoubleVectorAttributeValue `parquet:"name=value, type=LIST, valuetype=DOUBLE"`
}

func (_ *DoubleVectorAttribute) unorderedRow() interface{} {
	return new(UnorderedDoubleVectorAttributeRow)
}

func (s *Scalar) write(dirName string) error {
	fname := fmt.Sprintf("%v/serialized_data", dirName)
	f, err := os.Create(fname)
	defer f.Close()
	fw := bufio.NewWriter(f)
	if _, err := fw.Write([]byte(*s)); err != nil {
		return fmt.Errorf("Writing scalar to file failed: %v", err)
	}
	fw.Flush()
	successFile := fmt.Sprintf("%v/_SUCCESS", dirName)
	err = ioutil.WriteFile(successFile, nil, 0775)
	if err != nil {
		return fmt.Errorf("Failed to write success file: %v", err)
	}
	return nil
}
func readScalar(dirName string) (Scalar, error) {
	fname := fmt.Sprintf("%v/serialized_data", dirName)
	jsonEncoding, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file: %v", err)
	}
	return Scalar(jsonEncoding), nil
}
