package aiven

import (
	"encoding/json"
	"errors"
	"fmt"
)

type (
	KafkaAclsHandler struct {
		client *Client
	}

	CreateKafkaAclRequest struct {
		Permission *string `json:"permission"`
		Topic      *string `json:"topic"`
		Username   *string `json:"username"`
	}

	KafkaListAcl struct {
		Id *string `json:"id"`
		CreateKafkaAclRequest
	}

	KafkaAclResponse struct {
		APIResponse
		Acls []*KafkaListAcl `json:"acl"`
	}
)

// Create creats a specific kafka topic.
func (h *KafkaAclsHandler) Create(project, service string, req CreateKafkaAclRequest) ([]*KafkaListAcl, error) {
	rsp, err := h.client.doPostRequest(fmt.Sprintf("/project/%s/service/%s/acl", project, service), req)
	if err != nil {
		return nil, err
	}

	var response *KafkaAclResponse
	if err := json.Unmarshal(rsp, &response); err != nil {
		return nil, err
	}

	if len(response.Errors) != 0 {
		return nil, errors.New(response.Message)
	}

	return response.Acls, nil
}

// Delete deletes a specific kafka topic.
func (h *KafkaAclsHandler) Delete(project, service, aclid string) ([]*KafkaListAcl, error) {
	rsp, err := h.client.doDeleteRequest(fmt.Sprintf("/project/%s/service/%s/acl/%s", project, service, aclid), nil)
	if err != nil {
		return nil, err
	}

	var response *KafkaAclResponse
	if err := json.Unmarshal(rsp, &response); err != nil {
		return nil, err
	}

	if len(response.Errors) != 0 {
		return nil, errors.New(response.Message)
	}

	return response.Acls, nil
}
