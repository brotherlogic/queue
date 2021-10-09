// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.6.1
// source: queue.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Config struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Queues []string `protobuf:"bytes,1,rep,name=queues,proto3" json:"queues,omitempty"`
}

func (x *Config) Reset() {
	*x = Config{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Config) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Config) ProtoMessage() {}

func (x *Config) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Config.ProtoReflect.Descriptor instead.
func (*Config) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{0}
}

func (x *Config) GetQueues() []string {
	if x != nil {
		return x.Queues
	}
	return nil
}

type Queue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Name of the queue
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// The endpoint that will be called
	Endpoint string `protobuf:"bytes,2,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	// Deadline to be applied to any call
	Deadline int32 `protobuf:"varint,3,opt,name=deadline,proto3" json:"deadline,omitempty"`
	// The actual queue entries
	Entries []*Entry `protobuf:"bytes,4,rep,name=entries,proto3" json:"entries,omitempty"`
	// The type for the request
	Type string `protobuf:"bytes,5,opt,name=type,proto3" json:"type,omitempty"`
	// Max queue length
	MaxLength int32 `protobuf:"varint,6,opt,name=max_length,json=maxLength,proto3" json:"max_length,omitempty"`
	// Unique Keys or not
	UniqueKeys bool `protobuf:"varint,7,opt,name=unique_keys,json=uniqueKeys,proto3" json:"unique_keys,omitempty"`
}

func (x *Queue) Reset() {
	*x = Queue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Queue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Queue) ProtoMessage() {}

func (x *Queue) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Queue.ProtoReflect.Descriptor instead.
func (*Queue) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{1}
}

func (x *Queue) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Queue) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *Queue) GetDeadline() int32 {
	if x != nil {
		return x.Deadline
	}
	return 0
}

func (x *Queue) GetEntries() []*Entry {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *Queue) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Queue) GetMaxLength() int32 {
	if x != nil {
		return x.MaxLength
	}
	return 0
}

func (x *Queue) GetUniqueKeys() bool {
	if x != nil {
		return x.UniqueKeys
	}
	return false
}

type Entry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key     string     `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Payload *anypb.Any `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	RunTime int64      `protobuf:"varint,3,opt,name=run_time,json=runTime,proto3" json:"run_time,omitempty"`
}

func (x *Entry) Reset() {
	*x = Entry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Entry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entry) ProtoMessage() {}

func (x *Entry) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entry.ProtoReflect.Descriptor instead.
func (*Entry) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{2}
}

func (x *Entry) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Entry) GetPayload() *anypb.Any {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *Entry) GetRunTime() int64 {
	if x != nil {
		return x.RunTime
	}
	return 0
}

type AddQueueRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name     string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Endpoint string `protobuf:"bytes,2,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	Deadline int32  `protobuf:"varint,3,opt,name=deadline,proto3" json:"deadline,omitempty"`
	Type     string `protobuf:"bytes,4,opt,name=type,proto3" json:"type,omitempty"`
}

func (x *AddQueueRequest) Reset() {
	*x = AddQueueRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddQueueRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddQueueRequest) ProtoMessage() {}

func (x *AddQueueRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddQueueRequest.ProtoReflect.Descriptor instead.
func (*AddQueueRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{3}
}

func (x *AddQueueRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *AddQueueRequest) GetEndpoint() string {
	if x != nil {
		return x.Endpoint
	}
	return ""
}

func (x *AddQueueRequest) GetDeadline() int32 {
	if x != nil {
		return x.Deadline
	}
	return 0
}

func (x *AddQueueRequest) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

type AddQueueResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AddQueueResponse) Reset() {
	*x = AddQueueResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddQueueResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddQueueResponse) ProtoMessage() {}

func (x *AddQueueResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddQueueResponse.ProtoReflect.Descriptor instead.
func (*AddQueueResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{4}
}

type AddQueueItemRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	QueueName     string     `protobuf:"bytes,1,opt,name=queue_name,json=queueName,proto3" json:"queue_name,omitempty"`
	Key           string     `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	Payload       *anypb.Any `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
	RunTime       int64      `protobuf:"varint,4,opt,name=run_time,json=runTime,proto3" json:"run_time,omitempty"`
	RequireUnique bool       `protobuf:"varint,5,opt,name=require_unique,json=requireUnique,proto3" json:"require_unique,omitempty"`
}

func (x *AddQueueItemRequest) Reset() {
	*x = AddQueueItemRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddQueueItemRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddQueueItemRequest) ProtoMessage() {}

func (x *AddQueueItemRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddQueueItemRequest.ProtoReflect.Descriptor instead.
func (*AddQueueItemRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{5}
}

func (x *AddQueueItemRequest) GetQueueName() string {
	if x != nil {
		return x.QueueName
	}
	return ""
}

func (x *AddQueueItemRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *AddQueueItemRequest) GetPayload() *anypb.Any {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *AddQueueItemRequest) GetRunTime() int64 {
	if x != nil {
		return x.RunTime
	}
	return 0
}

func (x *AddQueueItemRequest) GetRequireUnique() bool {
	if x != nil {
		return x.RequireUnique
	}
	return false
}

type AddQueueItemResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AddQueueItemResponse) Reset() {
	*x = AddQueueItemResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddQueueItemResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddQueueItemResponse) ProtoMessage() {}

func (x *AddQueueItemResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddQueueItemResponse.ProtoReflect.Descriptor instead.
func (*AddQueueItemResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{6}
}

type DeleteQueueItemRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	QueueName string `protobuf:"bytes,1,opt,name=queue_name,json=queueName,proto3" json:"queue_name,omitempty"`
}

func (x *DeleteQueueItemRequest) Reset() {
	*x = DeleteQueueItemRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteQueueItemRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteQueueItemRequest) ProtoMessage() {}

func (x *DeleteQueueItemRequest) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteQueueItemRequest.ProtoReflect.Descriptor instead.
func (*DeleteQueueItemRequest) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{7}
}

func (x *DeleteQueueItemRequest) GetQueueName() string {
	if x != nil {
		return x.QueueName
	}
	return ""
}

type DeleteQueueItemResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *DeleteQueueItemResponse) Reset() {
	*x = DeleteQueueItemResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_queue_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteQueueItemResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteQueueItemResponse) ProtoMessage() {}

func (x *DeleteQueueItemResponse) ProtoReflect() protoreflect.Message {
	mi := &file_queue_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteQueueItemResponse.ProtoReflect.Descriptor instead.
func (*DeleteQueueItemResponse) Descriptor() ([]byte, []int) {
	return file_queue_proto_rawDescGZIP(), []int{8}
}

var File_queue_proto protoreflect.FileDescriptor

var file_queue_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x71,
	0x75, 0x65, 0x75, 0x65, 0x1a, 0x41, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x62, 0x75, 0x66, 0x66, 0x65, 0x72, 0x73,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x72, 0x63, 0x2f, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e,
	0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x20, 0x0a, 0x06, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x12, 0x16, 0x0a, 0x06, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x09, 0x52, 0x06, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x22, 0xcf, 0x01, 0x0a, 0x05, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x65, 0x6e, 0x64, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e, 0x64, 0x70, 0x6f,
	0x69, 0x6e, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x65, 0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x64, 0x65, 0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65, 0x12,
	0x26, 0x0a, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x0c, 0x2e, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07,
	0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x6d,
	0x61, 0x78, 0x5f, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x09, 0x6d, 0x61, 0x78, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0x12, 0x1f, 0x0a, 0x0b, 0x75, 0x6e,
	0x69, 0x71, 0x75, 0x65, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x0a, 0x75, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x4b, 0x65, 0x79, 0x73, 0x22, 0x64, 0x0a, 0x05, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2e, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61,
	0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x07, 0x70,
	0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x72, 0x75, 0x6e, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x72, 0x75, 0x6e, 0x54, 0x69, 0x6d,
	0x65, 0x22, 0x71, 0x0a, 0x0f, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x65, 0x6e, 0x64, 0x70,
	0x6f, 0x69, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x6e, 0x64, 0x70,
	0x6f, 0x69, 0x6e, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x65, 0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x64, 0x65, 0x61, 0x64, 0x6c, 0x69, 0x6e, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x74, 0x79, 0x70, 0x65, 0x22, 0x12, 0x0a, 0x10, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0xb8, 0x01, 0x0a, 0x13, 0x41, 0x64, 0x64,
	0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x1d, 0x0a, 0x0a, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x71, 0x75, 0x65, 0x75, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x2e, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61,
	0x64, 0x12, 0x19, 0x0a, 0x08, 0x72, 0x75, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x07, 0x72, 0x75, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e,
	0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x5f, 0x75, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x08, 0x52, 0x0d, 0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x55, 0x6e, 0x69,
	0x71, 0x75, 0x65, 0x22, 0x16, 0x0a, 0x14, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49,
	0x74, 0x65, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x37, 0x0a, 0x16, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x71, 0x75, 0x65, 0x75, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x71, 0x75, 0x65, 0x75, 0x65,
	0x4e, 0x61, 0x6d, 0x65, 0x22, 0x19, 0x0a, 0x17, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32,
	0xec, 0x01, 0x0a, 0x0c, 0x51, 0x75, 0x65, 0x75, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x12, 0x3d, 0x0a, 0x08, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x16, 0x2e, 0x71,
	0x75, 0x65, 0x75, 0x65, 0x2e, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x41, 0x64, 0x64,
	0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x49, 0x0a, 0x0c, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x12,
	0x1a, 0x2e, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x49, 0x74, 0x65, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1b, 0x2e, 0x71, 0x75,
	0x65, 0x75, 0x65, 0x2e, 0x41, 0x64, 0x64, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x52, 0x0a, 0x0f, 0x44, 0x65,
	0x6c, 0x65, 0x74, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x12, 0x1d, 0x2e,
	0x71, 0x75, 0x65, 0x75, 0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1e, 0x2e, 0x71,
	0x75, 0x65, 0x75, 0x65, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x51, 0x75, 0x65, 0x75, 0x65,
	0x49, 0x74, 0x65, 0x6d, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x25,
	0x5a, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x72, 0x6f,
	0x74, 0x68, 0x65, 0x72, 0x6c, 0x6f, 0x67, 0x69, 0x63, 0x2f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_queue_proto_rawDescOnce sync.Once
	file_queue_proto_rawDescData = file_queue_proto_rawDesc
)

func file_queue_proto_rawDescGZIP() []byte {
	file_queue_proto_rawDescOnce.Do(func() {
		file_queue_proto_rawDescData = protoimpl.X.CompressGZIP(file_queue_proto_rawDescData)
	})
	return file_queue_proto_rawDescData
}

var file_queue_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_queue_proto_goTypes = []interface{}{
	(*Config)(nil),                  // 0: queue.Config
	(*Queue)(nil),                   // 1: queue.Queue
	(*Entry)(nil),                   // 2: queue.Entry
	(*AddQueueRequest)(nil),         // 3: queue.AddQueueRequest
	(*AddQueueResponse)(nil),        // 4: queue.AddQueueResponse
	(*AddQueueItemRequest)(nil),     // 5: queue.AddQueueItemRequest
	(*AddQueueItemResponse)(nil),    // 6: queue.AddQueueItemResponse
	(*DeleteQueueItemRequest)(nil),  // 7: queue.DeleteQueueItemRequest
	(*DeleteQueueItemResponse)(nil), // 8: queue.DeleteQueueItemResponse
	(*anypb.Any)(nil),               // 9: google.protobuf.Any
}
var file_queue_proto_depIdxs = []int32{
	2, // 0: queue.Queue.entries:type_name -> queue.Entry
	9, // 1: queue.Entry.payload:type_name -> google.protobuf.Any
	9, // 2: queue.AddQueueItemRequest.payload:type_name -> google.protobuf.Any
	3, // 3: queue.QueueService.AddQueue:input_type -> queue.AddQueueRequest
	5, // 4: queue.QueueService.AddQueueItem:input_type -> queue.AddQueueItemRequest
	7, // 5: queue.QueueService.DeleteQueueItem:input_type -> queue.DeleteQueueItemRequest
	4, // 6: queue.QueueService.AddQueue:output_type -> queue.AddQueueResponse
	6, // 7: queue.QueueService.AddQueueItem:output_type -> queue.AddQueueItemResponse
	8, // 8: queue.QueueService.DeleteQueueItem:output_type -> queue.DeleteQueueItemResponse
	6, // [6:9] is the sub-list for method output_type
	3, // [3:6] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_queue_proto_init() }
func file_queue_proto_init() {
	if File_queue_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_queue_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Config); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Queue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Entry); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddQueueRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddQueueResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddQueueItemRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddQueueItemResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteQueueItemRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_queue_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteQueueItemResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_queue_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_queue_proto_goTypes,
		DependencyIndexes: file_queue_proto_depIdxs,
		MessageInfos:      file_queue_proto_msgTypes,
	}.Build()
	File_queue_proto = out.File
	file_queue_proto_rawDesc = nil
	file_queue_proto_goTypes = nil
	file_queue_proto_depIdxs = nil
}
