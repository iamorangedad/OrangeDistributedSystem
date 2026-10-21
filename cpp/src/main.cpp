#include <iostream>
#include <string>

int run_supervisor(int argc, char** argv);
int run_store(int argc, char** argv);
int run_gateway(int argc, char** argv);
int run_order(int argc, char** argv);
int run_recommender(int argc, char** argv);
int run_worker(int argc, char** argv);

int main(int argc, char** argv) {
    std::string command = "supervisor";
    int from = 1;
    if (argc > 1 && argv[1][0] != '-') {
        command = argv[1];
        from = 2;
    }
    int restc = argc - from + 1;
    char** restv = argv + from - 1;
    restv[0] = argv[0];
    if (command == "supervisor") return run_supervisor(restc, restv);
    if (command == "store") return run_store(restc, restv);
    if (command == "gateway") return run_gateway(restc, restv);
    if (command == "order") return run_order(restc, restv);
    if (command == "recommender") return run_recommender(restc, restv);
    if (command == "worker") return run_worker(restc, restv);
    std::cerr << "未知子命令 " << command << "。可用：supervisor、store、gateway、order、recommender、worker\n";
    return 2;
}
