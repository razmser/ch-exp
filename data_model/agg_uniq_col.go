package data_model

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"

	"github.com/ClickHouse/ch-go/proto"
)

// type ColResult interface {
// 	Type() ColumnType
// 	Rows() int
// 	DecodeColumn(r *Reader, rows int) error
// 	Resettable
// }

type AggregateUniqInt32 []ChUnique

func (a *AggregateUniqInt32) Type() proto.ColumnType {
	return "AggregateFunction(uniq, Int32)"
}

func (a *AggregateUniqInt32) Rows() int {
	return len(*a)
}

func (a *AggregateUniqInt32) DecodeColumn(r *proto.Reader, rows int) error {
	slog.Info("DecodeColumn")
	*a = make(AggregateUniqInt32, rows)
	for i := 0; i < rows; i++ {
		(*a)[i].ReadFrom(r)
	}
	return nil
}

func (a AggregateUniqInt32) Reset() {
	for i := range a {
		a[i].Reset()
	}
}

func (ch *ChUnique) ReadFrom(r *proto.Reader) error {
	ch.hasZeroItem = false
	sd, err := r.ReadByte()
	if err != nil {
		return err
	}
	ch.skipDegree = uint32(sd)
	ic, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if ic > uniquesHashMaxSize {
		return fmt.Errorf("ChUnique has too many (%d) items", ic)
	}
	ch.itemsCount = int32(ic)
	ch.sizeDegree = uniquesHashSetInitialSizeDegree
	if ic > 1 {
		ch.sizeDegree = uint32(math.Log2(float64(ic)) + 2)
		if ch.sizeDegree < uniquesHashSetInitialSizeDegree {
			ch.sizeDegree = uniquesHashSetInitialSizeDegree
		}
	}
	bufLen := 1 << ch.sizeDegree

	if cap(ch.buf) < bufLen {
		ch.buf = make([]uint32, bufLen)
	} else {
		ch.buf = ch.buf[:bufLen]
		for i := range ch.buf {
			ch.buf[i] = 0
		}
	}

	var tmp [4]byte
	for i := 0; i < int(ic); i++ {
		if _, err = r.Read(tmp[:]); err != nil {
			return err
		}
		x := binary.LittleEndian.Uint32(tmp[:])
		if x == 0 {
			ch.hasZeroItem = true
			continue
		}
		ch.reinsertImpl(x)
	}
	return nil
}
