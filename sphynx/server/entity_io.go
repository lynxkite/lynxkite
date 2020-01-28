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

type ParquetEntity interface { // Functions to write entities to and read them from Parquet format.
	orderedRow() interface{} // Object for ParquetReader and ParquetWriter to figure out the schema.
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
	SphynxId int64 `parquet:"name=sphynxId, type=INT64"`
	SparkId  int64 `parquet:"name=sparkId, type=INT64"`
}

func (_ *VertexSet) orderedRow() interface{} {
	return new(OrderedVertexRow)
}
func (v *VertexSet) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(v.MappingToUnordered))
	for i, sparkId := range v.MappingToUnordered {
		rows[i] = OrderedVertexRow{SphynxId: int64(i), SparkId: sparkId}
	}
	return rows
}
func (v *VertexSet) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedVertexRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	v.MappingToUnordered = make([]int64, numRows)
	for _, row := range rows {
		v.MappingToUnordered[int(row.SphynxId)] = row.SparkId
	}
	return nil
}

type OrderedEdgeRow struct {
	SphynxId int64 `parquet:"name=sphynxId, type=INT64"`
	Src      int64 `parquet:"name=src, type=INT64"`
	Dst      int64 `parquet:"name=dst, type=INT64"`
	SparkId  int64 `parquet:"name=sparkId, type=INT64"`
}

func (_ *EdgeBundle) orderedRow() interface{} {
	return new(OrderedEdgeRow)
}
func (eb *EdgeBundle) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(eb.Src))
	for i, v := range eb.EdgeMapping {
		rows[i] = OrderedEdgeRow{SphynxId: int64(i), Src: int64(eb.Src[i]), Dst: int64(eb.Dst[i]), SparkId: v}
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
	for _, row := range rows {
		i := int(row.SphynxId)
		eb.Src[i] = int(row.Src)
		eb.Dst[i] = int(row.Dst)
		eb.EdgeMapping[i] = row.SparkId
	}
	return nil
}

type OrderedStringAttributeRow struct {
	SphynxId int64  `parquet:"name=sphynxId, type=INT64"`
	Value    string `parquet:"name=value, type=UTF8"`
	Defined  bool   `parquet:"name=defined, type=BOOLEAN"`
}

func AttributeToOrderedRows(attr ParquetEntity) []interface{} {
	a := reflect.ValueOf(attr)
	values := a.Elem().FieldByName("Values")
	defined := a.Elem().FieldByName("Defined")
	numValues := values.Len()
	rows := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), numValues, numValues)
	rowType := reflect.Indirect(reflect.ValueOf(attr.orderedRow())).Type()
	for i := 0; i < numValues; i++ {
		row := reflect.New(rowType).Elem()
		row.FieldByName("SphynxId").SetInt(int64(i))
		row.FieldByName("Value").Set(values.Index(i))
		row.FieldByName("Defined").Set(defined.Index(i))
		rows.Index(i).Set(row)
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
		id := int(row.FieldByName("SphynxId").Int())
		values.Index(id).Set(row.FieldByName("Value"))
		defined.Index(id).Set(row.FieldByName("Defined"))
	}
	return nil
}

func (_ *StringAttribute) orderedRow() interface{} {
	return new(OrderedStringAttributeRow)
}

type OrderedDoubleAttributeRow struct {
	SphynxId int64   `parquet:"name=sphynxId, type=INT64"`
	Value    float64 `parquet:"name=value, type=DOUBLE"`
	Defined  bool    `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleAttribute) orderedRow() interface{} {
	return new(OrderedDoubleAttributeRow)
}

type OrderedDoubleTuple2AttributeRow struct {
	SphynxId int64                      `parquet:"name=sphynxId, type=INT64"`
	Value    DoubleTuple2AttributeValue `parquet:"name=value"`
	Defined  bool                       `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleTuple2Attribute) orderedRow() interface{} {
	return new(OrderedDoubleTuple2AttributeRow)
}

type Vertex struct {
	Id int64 `parquet:"name=id, type=INT64"`
}
type Edge struct {
	Id  int64 `parquet:"name=id, type=INT64"`
	Src int64 `parquet:"name=src, type=INT64"`
	Dst int64 `parquet:"name=dst, type=INT64"`
}
type SingleStringAttribute struct {
	Id    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}
type SingleDoubleAttribute struct {
	Id    int64   `parquet:"name=id, type=INT64"`
	Value float64 `parquet:"name=value, type=DOUBLE"`
}
type SingleDoubleTuple2Attribute struct {
	Id    int64                      `parquet:"name=id, type=INT64"`
	Value DoubleTuple2AttributeValue `parquet:"name=value"`
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
