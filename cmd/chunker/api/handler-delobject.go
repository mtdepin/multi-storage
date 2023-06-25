package api

import (
	"go.opencensus.io/trace"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
	"net/http"
	"strings"
)

// DeleteObjectHandler
func (h *chunkerAPIHandlers) DelObjectHandler(w http.ResponseWriter, r *http.Request) {
	logger.Info("===> DeleteObjectHandler")
	ctx, span := trace.StartSpan(r.Context(), "PostObjectHandler")
	defer span.End()

	cid := strings.Split(r.URL.EscapedPath(), "/")[4]
	if err := h.backend.DeleteDataFromIPFS(ctx, cid); err != nil {
		if strings.Contains(err.Error(), "not pinned or pinned indirectly") {
			util.WriteJsonQuiet(w, http.StatusNotFound, "not found")
			return
		}
		util.WriteJsonQuiet(w, http.StatusInternalServerError, err.Error())
		return
	}
	util.WriteJsonQuiet(w, http.StatusOK, "success")
	return
}
