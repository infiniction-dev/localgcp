// Package lro builds long-running operations for the Cloud Run emulator.
package lro

import (
	"fmt"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Completed wraps a result in an immediately-completed long-running operation.
// The result is embedded as the operation Response so that client .Wait() calls
// return without ever polling GetOperation.
func Completed(opName string, result proto.Message) (*longrunning.Operation, error) {
	any, err := anypb.New(result)
	if err != nil {
		return nil, fmt.Errorf("marshal operation result: %w", err)
	}
	return &longrunning.Operation{
		Name:   opName,
		Done:   true,
		Result: &longrunning.Operation_Response{Response: any},
	}, nil
}
