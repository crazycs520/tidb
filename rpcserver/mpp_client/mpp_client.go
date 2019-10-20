package mpp_client

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/mpp_processor"
	"github.com/pingcap/kvproto/pkg/tidbpb"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"time"
)

func SendRPCToALLServer(sctx sessionctx.Context, req *mpp_processor.Request, chk *chunk.Chunk, fieldTypes []*types.FieldType) error {
	servers, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return err
	}
	for _, ser := range servers {
		err := SendRPC(sctx, ser.IP+":"+strconv.FormatUint(uint64(ser.StatusPort), 10), req, chk, fieldTypes)
		if err != nil {
			return err
		}
	}
	return nil
}

func SendRPC(sctx sessionctx.Context, address string, req *mpp_processor.Request, chk *chunk.Chunk, fieldTypes []*types.FieldType) error {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := tidbpb.NewTidbClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.MppProcessor(ctx, req)
	if err != nil {
		return err
	}
	return readRowsData(sctx, r, chk, fieldTypes)
}

func readRowsData(ctx sessionctx.Context, resp *mpp_processor.Response, chk *chunk.Chunk, fieldTypes []*types.FieldType) (err error) {
	selResp := new(tipb.SelectResponse)
	err = proto.Unmarshal(resp.Data, selResp)
	if err != nil {
		return err
	}

	if len(selResp.Chunks) < 1 {
		return nil
	}
	decoder := codec.NewDecoder(chk, ctx.GetSessionVars().Location())
	for _, chunk := range selResp.Chunks {
		rowsData := chunk.RowsData
		for len(rowsData) > 0 {
			for i := 0; i < len(fieldTypes); i++ {
				rowsData, err = decoder.DecodeOne(rowsData, i, fieldTypes[i])
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
