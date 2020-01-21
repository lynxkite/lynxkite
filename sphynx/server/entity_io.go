// Helper methods to read and write entities.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/xitongsys/parquet-go/reader"
	"io/ioutil"
	"os"
)

type Entity interface {
	typeName() string // This will help deserializing a serialized entity
}

type ParquetEntity interface { // Functions to write entities to and read them from Parquet format.
	orderedRow() interface{} // Object for ParquetReader and ParquetWriter to figure out the schema.
	toOrderedRows() []interface{}
	readFromOrdered(*reader.ParquetReader, int) error
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

func (_ *StringAttribute) orderedRow() interface{} {
	return new(OrderedStringAttributeRow)
}
func (a *StringAttribute) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(a.Values))
	for i, v := range a.Values {
		rows[i] = OrderedStringAttributeRow{SphynxId: int64(i), Value: v, Defined: a.Defined[i]}
	}
	return rows
}
func (a *StringAttribute) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedStringAttributeRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	a.Values = make([]string, numRows)
	a.Defined = make([]bool, numRows)
	for _, row := range rows {
		i := int(row.SphynxId)
		a.Values[i] = row.Value
		a.Defined[i] = row.Defined
	}
	return nil
}

type OrderedDoubleAttributeRow struct {
	SphynxId int64   `parquet:"name=sphynxId, type=INT64"`
	Value    float64 `parquet:"name=value, type=DOUBLE"`
	Defined  bool    `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleAttribute) orderedRow() interface{} {
	return new(OrderedDoubleAttributeRow)
}
func (a *DoubleAttribute) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(a.Values))
	for i, v := range a.Values {
		rows[i] = OrderedDoubleAttributeRow{SphynxId: int64(i), Value: v, Defined: a.Defined[i]}
	}
	return rows
}
func (a *DoubleAttribute) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedDoubleAttributeRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	a.Values = make([]float64, numRows)
	a.Defined = make([]bool, numRows)
	for _, row := range rows {
		i := int(row.SphynxId)
		a.Values[i] = row.Value
		a.Defined[i] = row.Defined
	}
	return nil
}

type OrderedDoubleTuple2AttributeRow struct {
	SphynxId int64                      `parquet:"name=sphynxId, type=INT64"`
	Value    DoubleTuple2AttributeValue `parquet:"name=value"`
	Defined  bool                       `parquet:"name=defined, type=BOOLEAN"`
}

func (_ *DoubleTuple2Attribute) orderedRow() interface{} {
	return new(OrderedDoubleTuple2AttributeRow)
}
func (a *DoubleTuple2Attribute) toOrderedRows() []interface{} {
	rows := make([]interface{}, len(a.Values))
	for i, v := range a.Values {
		rows[i] = OrderedDoubleTuple2AttributeRow{SphynxId: int64(i), Value: v, Defined: a.Defined[i]}
	}
	return rows
}
func (a *DoubleTuple2Attribute) readFromOrdered(pr *reader.ParquetReader, numRows int) error {
	rows := make([]OrderedDoubleTuple2AttributeRow, numRows)
	if err := pr.Read(&rows); err != nil {
		return fmt.Errorf("Failed to read parquet file: %v", err)
	}
	a.Values = make([]DoubleTuple2AttributeValue, numRows)
	a.Defined = make([]bool, numRows)
	for _, row := range rows {
		i := int(row.SphynxId)
		a.Values[i] = row.Value
		a.Defined[i] = row.Defined
	}
	return nil
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
	scalarJSON, err := json.Marshal(s.Value)
	if err != nil {
		return fmt.Errorf("Converting scalar to json failed: %v", err)
	}
	fname := fmt.Sprintf("%v/serialized_data", dirName)
	f, err := os.Create(fname)
	fw := bufio.NewWriter(f)
	if _, err := fw.WriteString(string(scalarJSON)); err != nil {
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
