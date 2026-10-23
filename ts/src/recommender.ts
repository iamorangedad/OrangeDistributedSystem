import { errorBody, faultDelay, logEvent, setContext, startFaultReader } from "./common";
import { emptyResponse, headerRequestID, jsonResponse, serve, splitPath, textResponse, type Handler } from "./http";

function recommendationFor(sku: string): string {
  if (sku === "gear-1") return "配套护具";
  return "相关:" + sku;
}

function handler(): Handler {
  return async (method, path, headers) => {
    const requestID = headerRequestID(headers);
    const [route, query] = splitPath(path);
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/ready") return emptyResponse(200);
    if (method === "GET" && route === "/metrics") return textResponse(200, "orange_errors_total 0\n");
    if (method === "GET" && route === "/internal/recommend") {
      const sku = query["sku"] || "";
      if (!sku) return jsonResponse(400, errorBody("invalid_argument", "缺少 sku", requestID));
      logEvent("recommend_served", requestID, "info", { sku });
      await faultDelay("recommender");
      return jsonResponse(200, { recommendation: recommendationFor(sku) });
    }
    return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID));
  };
}

export async function runRecommender(flags: Map<string, string>): Promise<void> {
  const port = Number(flags.get("port") ?? "8200");
  const root = flags.get("root") ?? ".";
  const name = flags.get("process-name") ?? "recommender";
  setContext(name, root);
  startFaultReader();
  logEvent("process_started", "", "info", { port });
  await serve(port, handler());
}
