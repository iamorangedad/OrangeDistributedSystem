package orange;

/** 一个入口，用子命令区分进程，方便和 Python、Rust 对照着读。 */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        String command = "supervisor";
        int from = 0;
        if (args.length > 0 && !args[0].startsWith("-")) {
            command = args[0];
            from = 1;
        }
        String[] rest = java.util.Arrays.copyOfRange(args, from, args.length);
        switch (command) {
            case "supervisor" -> Supervisor.run(rest);
            case "store" -> Store.run(rest);
            case "gateway" -> Gateway.run(rest);
            case "order" -> OrderService.run(rest);
            case "recommender" -> Recommender.run(rest);
            case "worker" -> Worker.run(rest);
            default -> {
                System.err.println("未知子命令 " + command + "。可用：supervisor、store、gateway、order、recommender、worker");
                System.exit(2);
            }
        }
    }
}
