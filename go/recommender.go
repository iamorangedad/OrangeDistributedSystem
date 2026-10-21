package main

func recommendationFor(sku string) string {
	if sku == "gear-1" {
		return "配套护具"
	}
	return "相关:" + sku
}

func recommenderHandler() handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		requestID := headerRequestID(headers)
		route, query := splitPath(path)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/ready":
			return emptyResponse(200)
		case method == "GET" && route == "/metrics":
			return textResponse(200, "orange_errors_total 0\n")
		case method == "GET" && route == "/internal/recommend":
			sku := query["sku"]
			if sku == "" {
				return jsonResponse(400, errorBody("invalid_argument", "缺少 sku", requestID), nil)
			}
			logEvent("recommend_served", requestID, "info", map[string]any{"sku": sku})
			faultDelay("recommender")
			return jsonResponse(200, map[string]any{"recommendation": recommendationFor(sku)}, nil)
		default:
			return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID), nil)
		}
	}
}

func runRecommender(args []string) {
	flags := newFlagSet()
	port := flags.Int("port", 8200, "")
	root := flags.String("root", ".", "")
	name := flags.String("process-name", "recommender", "")
	parseFlags(flags, args)
	setContext(*name, *root)
	startFaultReader()
	logEvent("process_started", "", "info", map[string]any{"port": *port})
	if err := serve(*port, recommenderHandler()); err != nil {
		panic(err)
	}
}
