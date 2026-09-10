/* Synthetic app for installer tests: no database or inference connections. */
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#ifndef VERSION
#define VERSION "0.0.0"
#endif
#ifndef MODE
#define MODE "hybrid"
#endif
int main(int argc, char **argv) {
    if (argc == 2 && strcmp(argv[1], "--version") == 0) {
        printf("hn_app %s+fixture\n", VERSION);
        return 0;
    }
    int server = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=htons(18883),
        .sin_addr.s_addr=htonl(INADDR_LOOPBACK)};
    if (bind(server, (void *)&addr, sizeof(addr)) || listen(server, 8)) return 1;
    for (;;) {
        int client = accept(server, NULL, NULL);
        if (client < 0) return 1;
        char request[2048] = {0}, response[4096];
        read(client, request, sizeof(request)-1);
        const char *body = strstr(request, "GET /health ") ? "ok" :
            "{\"retrieval_mode\":\"" MODE "\",\"results\":[{\"id\":1}]}";
        int size = snprintf(response, sizeof(response),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s", strlen(body), body);
        write(client, response, size);
        close(client);
    }
}
