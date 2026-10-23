import { installSignals, stopped } from "./http";
import { runGateway } from "./gateway";
import { runOrder } from "./order";
import { runRecommender } from "./recommender";
import { runStore } from "./store";
import { runSupervisor } from "./supervisor";
import { runWorker } from "./worker";

function parseArgs(argv: string[]): { command: string; flags: Map<string, string> } {
  let command = "supervisor";
  let index = 0;
  if (argv.length > 0 && !argv[0].startsWith("-")) {
    command = argv[0];
    index = 1;
  }
  const flags = new Map<string, string>();
  for (; index < argv.length; index++) {
    const arg = argv[index];
    if (!arg.startsWith("--")) continue;
    const body = arg.slice(2);
    const eq = body.indexOf("=");
    if (eq >= 0) flags.set(body.slice(0, eq), body.slice(eq + 1));
    else flags.set(body, argv[++index] ?? "");
  }
  return { command, flags };
}

async function main(): Promise<void> {
  installSignals();
  const { command, flags } = parseArgs(process.argv.slice(2));
  if (command === "supervisor") await runSupervisor(flags);
  else if (command === "store") await runStore(flags);
  else if (command === "gateway") await runGateway(flags);
  else if (command === "order") await runOrder(flags);
  else if (command === "recommender") await runRecommender(flags);
  else if (command === "worker") await runWorker(flags);
  else {
    console.error(`未知子命令 ${command}。可用：supervisor、store、gateway、order、recommender、worker`);
    process.exit(2);
  }
  if (stopped()) process.exit(0);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : error);
  process.exit(1);
});
