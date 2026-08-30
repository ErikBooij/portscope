package httpadapter

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

const maxElasticItems = 100

func applySemanticProfile(upstream config.Upstream, item observation.Interaction) observation.Interaction {
	if upstream.Protocol != "elasticsearch" {
		return item
	}
	item.Protocol = "elasticsearch"
	api, index := elasticOperation(item.Attributes["method"], item.Attributes["path"])
	item.Operation = api
	if index != "" {
		item.Operation += " " + index
		item.Attributes["api"] = strings.ToLower(api)
		item.Attributes["index"] = index
	} else {
		item.Attributes["api"] = strings.ToLower(api)
	}
	item.Request.Summary = item.Operation
	if api == "BULK" || api == "MSEARCH" {
		item.Request = elasticNDJSONPayload(item.Request, api)
	}
	item = summarizeElasticResponse(item, api)
	if product := elasticProduct(item.Response.Headers); product != "" {
		item.Attributes["product"] = product
	}
	return item
}

func elasticOperation(method, path string) (string, string) {
	decoded, err := url.PathUnescape(path)
	if err == nil {
		path = decoded
	}
	segments := strings.Split(strings.Trim(path, "/"), "/")
	if len(segments) == 1 && segments[0] == "" {
		if method == "GET" {
			return "INFO", ""
		}
		return "ELASTIC " + method, ""
	}
	index := ""
	if len(segments) > 0 && !strings.HasPrefix(segments[0], "_") {
		index = segments[0]
	}
	endpoint := ""
	for position, segment := range segments {
		if strings.HasPrefix(segment, "_") {
			endpoint = strings.Join(segments[position:], "/")
			break
		}
	}
	switch endpoint {
	case "_search":
		return "SEARCH", index
	case "_msearch", "_msearch/template":
		return "MSEARCH", index
	case "_bulk":
		return "BULK", index
	case "_count":
		return "COUNT", index
	case "_mget":
		return "MGET", index
	case "_search/scroll":
		return "SCROLL", index
	case "_update_by_query":
		return "UPDATE BY QUERY", index
	case "_delete_by_query":
		return "DELETE BY QUERY", index
	case "_reindex":
		return "REINDEX", index
	case "_cluster/health":
		return "CLUSTER HEALTH", ""
	}
	if strings.HasPrefix(endpoint, "_cat/") {
		return "CAT " + strings.ToUpper(strings.TrimPrefix(endpoint, "_cat/")), ""
	}
	if len(segments) >= 2 && (segments[1] == "_doc" || segments[1] == "_create" || segments[1] == "_update") {
		switch segments[1] {
		case "_create":
			return "CREATE", index
		case "_update":
			return "UPDATE", index
		default:
			switch method {
			case "GET", "HEAD":
				return "GET", index
			case "DELETE":
				return "DELETE", index
			default:
				return "INDEX", index
			}
		}
	}
	return "ELASTIC " + method, index
}

func elasticNDJSONPayload(payload observation.Payload, api string) observation.Payload {
	if payload.Kind != "text" || payload.Text == "" {
		return payload
	}
	lines := strings.Split(payload.Text, "\n")
	values := make([]any, 0, min(len(lines), maxElasticItems))
	truncated := payload.Truncated
	for line := 0; line < len(lines); {
		if strings.TrimSpace(lines[line]) == "" {
			line++
			continue
		}
		var first map[string]any
		if err := json.Unmarshal([]byte(lines[line]), &first); err != nil {
			return payload
		}
		line++
		if len(values) >= maxElasticItems {
			truncated = true
			continue
		}
		if api == "MSEARCH" {
			entry := map[string]any{"header": first}
			if line < len(lines) && strings.TrimSpace(lines[line]) != "" {
				var query any
				if err := json.Unmarshal([]byte(lines[line]), &query); err != nil {
					return payload
				}
				entry["query"] = query
				line++
			}
			values = append(values, entry)
			continue
		}
		action, metadata := firstEntry(first)
		entry := map[string]any{"action": action, "metadata": metadata}
		if action != "delete" && line < len(lines) && strings.TrimSpace(lines[line]) != "" {
			var document any
			if err := json.Unmarshal([]byte(lines[line]), &document); err != nil {
				return payload
			}
			entry["document"] = document
			line++
		}
		values = append(values, entry)
	}
	encoded, err := json.Marshal(map[string]any{"items": values, "count": len(values), "captureTruncated": truncated})
	if err != nil || len(encoded) > captureLimit {
		return payload
	}
	payload.Kind, payload.Text, payload.JSON, payload.Truncated = "json", "", encoded, truncated
	payload.Summary = fmt.Sprintf("%s · %d items", payload.Summary, len(values))
	return payload
}

func summarizeElasticResponse(item observation.Interaction, api string) observation.Interaction {
	if len(item.Response.JSON) == 0 {
		return item
	}
	var body map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(item.Response.JSON)))
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil {
		return item
	}
	if took, ok := integer(body["took"]); ok {
		item.Attributes["serverTookMs"] = strconv.FormatInt(took, 10)
	}
	if timedOut, ok := body["timed_out"].(bool); ok {
		item.Attributes["timedOut"] = strconv.FormatBool(timedOut)
	}
	if version, ok := body["version"].(map[string]any); ok {
		if number, ok := version["number"].(string); ok {
			item.Attributes["version"] = number
		}
		if distribution, ok := version["distribution"].(string); ok {
			item.Attributes["product"] = distribution
		}
	}
	if result, ok := body["result"].(string); ok {
		item.Attributes["result"] = result
		item.Response.Summary = result
	}
	if documentIndex, ok := body["_index"].(string); ok && item.Attributes["index"] == "" {
		item.Attributes["index"] = documentIndex
	}
	if id, ok := body["_id"].(string); ok {
		item.Attributes["documentId"] = id
	}
	if shards, ok := body["_shards"].(map[string]any); ok {
		if total, ok := integer(shards["total"]); ok {
			item.Attributes["shards"] = strconv.FormatInt(total, 10)
		}
		if failed, ok := integer(shards["failed"]); ok {
			item.Attributes["failedShards"] = strconv.FormatInt(failed, 10)
		}
	}
	switch api {
	case "SEARCH":
		if hits, ok := body["hits"].(map[string]any); ok {
			count, relation := elasticHits(hits["total"])
			item.Attributes["hits"] = strconv.FormatInt(count, 10)
			if relation != "" {
				item.Attributes["hitRelation"] = relation
			}
			item.Response.Summary = fmt.Sprintf("%s%d hits%s", relationPrefix(relation), count, tookSuffix(item.Attributes))
		}
	case "COUNT":
		if count, ok := integer(body["count"]); ok {
			item.Attributes["count"] = strconv.FormatInt(count, 10)
			item.Response.Summary = fmt.Sprintf("%d documents%s", count, tookSuffix(item.Attributes))
		}
	case "BULK":
		items, _ := body["items"].([]any)
		failures := elasticBulkFailures(items)
		item.Attributes["bulkItems"] = strconv.Itoa(len(items))
		item.Attributes["bulkFailures"] = strconv.Itoa(failures)
		item.Response.Summary = fmt.Sprintf("%d operations · %d failed%s", len(items), failures, tookSuffix(item.Attributes))
		if failures > 0 {
			item.Outcome, item.Error = "error", fmt.Sprintf("%d bulk operations failed", failures)
		}
	case "MSEARCH":
		responses, _ := body["responses"].([]any)
		failures := elasticSearchFailures(responses)
		item.Attributes["searches"] = strconv.Itoa(len(responses))
		item.Attributes["searchFailures"] = strconv.Itoa(failures)
		item.Response.Summary = fmt.Sprintf("%d searches · %d failed%s", len(responses), failures, tookSuffix(item.Attributes))
		if failures > 0 {
			item.Outcome, item.Error = "error", fmt.Sprintf("%d searches failed", failures)
		}
	}
	if reason := elasticErrorReason(body["error"]); reason != "" {
		item.Outcome, item.Error = "error", reason
		item.Response.Summary = reason
	}
	return item
}

func firstEntry(value map[string]any) (string, any) {
	for key, item := range value {
		return key, item
	}
	return "unknown", map[string]any{}
}

func elasticHits(value any) (int64, string) {
	if count, ok := integer(value); ok {
		return count, "eq"
	}
	if object, ok := value.(map[string]any); ok {
		count, _ := integer(object["value"])
		relation, _ := object["relation"].(string)
		return count, relation
	}
	return 0, ""
}

func integer(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		result, err := typed.Int64()
		return result, err == nil
	case float64:
		return int64(typed), true
	case int:
		return int64(typed), true
	default:
		return 0, false
	}
}

func elasticBulkFailures(items []any) int {
	failures := 0
	for _, raw := range items {
		entry, _ := raw.(map[string]any)
		_, value := firstEntry(entry)
		result, _ := value.(map[string]any)
		status, _ := integer(result["status"])
		if status >= 300 || result["error"] != nil {
			failures++
		}
	}
	return failures
}

func elasticSearchFailures(responses []any) int {
	failures := 0
	for _, raw := range responses {
		entry, _ := raw.(map[string]any)
		status, _ := integer(entry["status"])
		if status >= 300 || entry["error"] != nil {
			failures++
		}
	}
	return failures
}

func elasticErrorReason(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case map[string]any:
		if reason, ok := typed["reason"].(string); ok && reason != "" {
			return reason
		}
		if causes, ok := typed["root_cause"].([]any); ok && len(causes) > 0 {
			return elasticErrorReason(causes[0])
		}
	}
	return ""
}

func elasticProduct(headers []observation.Pair) string {
	for _, header := range headers {
		if strings.EqualFold(header.Name, "X-Elastic-Product") && header.Value != "" {
			return header.Value
		}
		if strings.EqualFold(header.Name, "X-OpenSearch-Version") {
			return "OpenSearch"
		}
	}
	return ""
}

func relationPrefix(relation string) string {
	if relation == "gte" {
		return "≥"
	}
	return ""
}

func tookSuffix(attributes map[string]string) string {
	if value := attributes["serverTookMs"]; value != "" {
		return " · " + value + "ms server"
	}
	return ""
}
