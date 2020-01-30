// Helper methods to read and write entities.
package main

import (
	"bufio"
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"os"
	"reflect"
)

type Entity interface {
	typeName() string // This will help deserializing a serialized entity
}

type ParquetEntity interface { // Get objects for ParquetReader and ParquetWriter to figure out the schema.
	orderedRow() interface{}
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
func (_ *DoubleTuple2Attribute) typeName() string {
	return "DoubleTuple2Attribute"
}

type OrderedVertexRow struct {
	SparkId int64 `parquet:"name=sparkId, type=INT64"`
}

func (_ *VertexSet) orderedRow() interface{} {
	return new(OrderedVertexRow)
}
func (v *VertexSet) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(v.MappingToUnordered))
	for i, sparkId := range v.MappingToUnordered {
		rows[i] = OrderedVertexRow{SparkId: sparkId}
	}
	return rows
}
func (v *VertexSet) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedVertexRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	v.MappingToUnordered = make([]int64, numRows)
	for i, row := range rows {
		v.MappingToUnordered[i] = row.SparkId
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

type OrderedEdgeRow struct {
	Src     int64 `parquet:"name=src, type=INT64"`
	Dst     int64 `parquet:"name=dst, type=INT64"`
	SparkId int64 `parquet:"name=sparkId, type=INT64"`
}

func (_ *EdgeBundle) orderedRow() interface{} {
	return new(OrderedEdgeRow)
}
func (eb *EdgeBundle) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(eb.Src))
	for i, v := range eb.EdgeMapping {
		rows[i] = OrderedEdgeRow{Src: int64(eb.Src[i]), Dst: int64(eb.Dst[i]), SparkId: v}
	}
	return rows
}
func (eb *EdgeBundle) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedEdgeRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	eb.Src = make([]int, numRows)
	eb.Dst = make([]int, numRows)
	eb.EdgeMapping = make([]int64, numRows)
	for i, row := range rows {
		eb.Src[i] = int(row.Src)
		eb.Dst[i] = int(row.Dst)
		eb.EdgeMapping[i] = row.SparkId
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

func AttributeToOrderedRows(attr ParquetEntity) []interface{} {
	a := reflect.ValueOf(attr)
	values := a.Elem().FieldByName("Values")
	defined := a.Elem().FieldByName("Defined")
	numValues := values.Len()
	rows := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), numValues, numValues)
	rowType := reflect.TypeOf(attr.orderedRow()).Elem()
	fmt.Println("rowType", rowType)
	valueIndex := fieldIndex(rowType, "Value")
	definedIndex := fieldIndex(rowType, "Defined")
	row := reflect.New(rowType).Elem()
	for i := 0; i < numValues; i++ {
		row.Field(valueIndex).Set(values.Index(i))
		row.Field(definedIndex).Set(defined.Index(i))
		rows.Index(i).Set(row)
	}
	return rows.Interface().([]interface{})
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

func ReadAttributeFromOrdered(origAttr ParquetEntity, pr *reader.ParquetReader, numRows int) error {
	attr := reflect.ValueOf(origAttr)
	InitializeAttribute(attr, numRows)
	rowType := reflect.Indirect(reflect.ValueOf(origAttr.orderedRow())).Type()
	rowSliceType := reflect.SliceOf(rowType)
	rowsPointer := reflect.New(rowSliceType)
	rows := rowsPointer.Elem()
	rows.Set(reflect.MakeSlice(rowSliceType, numRows, numRows))
	if err := pr.Read(rowsPointer.Interface()); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	values := attr.Elem().FieldByName("Values")
	defined := attr.Elem().FieldByName("Defined")
	for i := 0; i < numRows; i++ {
		row := rows.Index(i)
		values.Index(i).Set(row.FieldByName("Value"))
		defined.Index(i).Set(row.FieldByName("Defined"))
	}
	return nil
}

type OrderedStringAttributeRow struct {
	Value   string `parquet:"name=value, type=UTF8"`
	Defined bool   `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *StringAttribute) orderedRow() interface{} {
	return new(OrderedStringAttributeRow)
}

type UnorderedStringAttributeRow struct {
	Id    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}

func (_ *StringAttribute) unorderedRow() interface{} {
	return new(UnorderedStringAttributeRow)
}

type OrderedDoubleAttributeRow struct {
	Value   float64 `parquet:"name=value, type=DOUBLE"`
	Defined bool    `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleAttribute) orderedRow() interface{} {
	return new(OrderedDoubleAttributeRow)
}

type UnorderedDoubleAttributeRow struct {
	Id    int64   `parquet:"name=id, type=INT64"`
	Value float64 `parquet:"name=value, type=DOUBLE"`
}

func (_ *DoubleAttribute) unorderedRow() interface{} {
	return new(UnorderedDoubleAttributeRow)
}

type OrderedDoubleTuple2AttributeRow struct {
	Value   DoubleTuple2AttributeValue `parquet:"name=value"`
	Defined bool                       `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleTuple2Attribute) orderedRow() interface{} {
	return new(OrderedDoubleTuple2AttributeRow)
}

type UnorderedDoubleTuple2AttributeRow struct {
	Id    int64                      `parquet:"name=id, type=INT64"`
	Value DoubleTuple2AttributeValue `parquet:"name=value"`
}

func (_ *DoubleTuple2Attribute) unorderedRow() interface{} {
	return new(UnorderedDoubleTuple2AttributeRow)
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
