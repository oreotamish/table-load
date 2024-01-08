package utils

import (
	"strings"
	"time"
)

func GetNestedValue(doc interface{}, keys []string) interface{} {
	for _, key := range keys {
		switch val := doc.(type) {
		case map[string]interface{}:
			doc = val[key]
		case []interface{}:
			var temp []interface{}
			for _, item := range val {
				if nestedVal, ok := item.(map[string]interface{}); ok {
					if nestedVal[key] != nil {
						temp = append(temp, nestedVal[key])
					}
				}
			}
			doc = temp
		default:
			return nil
		}
	}
	return doc
}

func ExtractData(doc []map[string]interface{}, columnMap map[string]string) ([]map[string]interface{}, error) {
	var extracted []map[string]interface{}
	for _, document := range doc {
		extractedDoc := make(map[string]interface{})
		for mongoCol, redshiftCol := range columnMap {
			if val, ok := document[mongoCol]; ok {
				if strVal, isMap := val.(map[string]interface{}); isMap && strVal["$date"] != nil {
					dateString := strVal["$date"].(string)
					layout := "2006-01-02T15:04:05.999Z"
					datetime, err := time.Parse(layout, dateString)
					if err != nil {
						extractedDoc[redshiftCol] = val
					} else {
						extractedDoc[redshiftCol] = datetime.Format("2006-01-02 15:04:05")
					}
				} else {
					extractedDoc[redshiftCol] = val
				}
			} else if strings.Contains(mongoCol, ".") {
				nestedKeys := strings.Split(mongoCol, ".")
				if nestedValue := GetNestedValue(document, nestedKeys); nestedValue != nil {
					if len(nestedValue.([]interface{})) > 0 {
						extractedDoc[redshiftCol] = nestedValue.([]interface{})[0]
					} else {
						extractedDoc[redshiftCol] = ""
					}
				}
			}
		}
		extracted = append(extracted, extractedDoc)
	}
	return extracted, nil
}
