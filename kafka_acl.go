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
		Permission		      *string `json:"permission"`
		Topic				  *string `json:"topic"`
		Username              *string `json:"username"`
	}

	KafkaListAcl struct {
		Id				      *string `json:"id"`
		CreateKafkaAclRequest
	}


	KafkaAclResponse struct {
		APIResponse
		Acls []*KafkaListAcl `json:"acl"`
	}
)

// Create creats a specific kafka topic.
func (h *KafkaAclsHandler) Create(project, service string, req CreateKafkaAclRequest) error {
	bts, err := h.client.doPostRequest(fmt.Sprintf("/project/%s/service/%s/acl", project, service), req)
	if err != nil {
		return err
	}

	var rsp *APIResponse
	if err := json.Unmarshal(bts, &rsp); err != nil {
		return err
	}

	if rsp == nil {
		return ErrNoResponseData
	}

	if rsp.Errors != nil && len(rsp.Errors) != 0 {
		return errors.New(rsp.Message)
	}

	return nil
}



// Delete deletes a specific kafka topic.
func (h *KafkaAclsHandler) Delete(project, service, aclid string) error {
	bts, err := h.client.doDeleteRequest(fmt.Sprintf("/project/%s/service/%s/acl/%s", project, service, aclid), nil)
	if err != nil {
		return err
	}

	return handleDeleteResponse(bts)
}
