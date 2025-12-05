// &Output: run.me

#include <iostream>
#include <signal.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <climits>
#include <cstring>
#include <cerrno>
using namespace std;

volatile sig_atomic_t wasSigHup = 0;
void sigHupHandler(int r)
{
	wasSigHup = 1;
}

int main()
{
    // Настройка обработчкика сигналов
	struct sigaction sa;
	sigaction(SIGHUP, NULL, &sa);
	sa.sa_handler = sigHupHandler;
	sa.sa_flags |= SA_RESTART;
	sigaction(SIGHUP, &sa, NULL);

	sigset_t blockedMask;
    sigset_t origMask;
	sigemptyset(&blockedMask);
	sigaddset(&blockedMask, SIGHUP);
	sigprocmask(SIG_BLOCK, &blockedMask, &origMask);

    // Создание и настройка сокета
	int sock = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in sock_addr;
	memset(&sock_addr, 0, sizeof(sock_addr));
	sock_addr.sin_family = AF_INET;
	sock_addr.sin_port = htons(5090);
	sock_addr.sin_addr.s_addr = INADDR_ANY;
	bind(sock, (struct sockaddr *) &sock_addr, sizeof(sock_addr));
	listen(sock, 2);

	int client = -1;
	int counter = 0;
	while (true)
    {
		if (wasSigHup == 1)
        {
			cout << "Получен сигнал SIGHUP" << endl;
			wasSigHup = 0;
		}
		int maxFd = sock;
		fd_set fds;
		FD_ZERO(&fds);
		FD_SET(sock, &fds);
		if (client != -1)
        {
			FD_SET(client, &fds);
		}
		if (maxFd < client)
        {
			maxFd = client;
		}
		if (pselect(maxFd + 1, &fds, NULL, NULL, NULL, &origMask) == -1)
        {
			if (errno == EINTR)
				continue;
            else
            {
				cout << "Ошибка pselect()" << endl;
				exit(1);
			}
		}
		if (FD_ISSET(sock, &fds))
        {
			int new_client = accept(sock, NULL, NULL);
			if (new_client != -1)
            {
				cout << "Принято новое подключение: " << new_client << endl;
				counter += 1;
				if (counter > 1)
                {
					close(new_client);
					cout << "Подключение " << new_client << " закрыто" << endl;
					counter -= 1;
				}
                else
					client = new_client;
			}	
		}
		if (client != -1 && FD_ISSET(client, &fds))
        {
			char buffer[256];
			int msg_size = read(client, buffer, sizeof(buffer));
            cout << buffer << endl;
			if (msg_size > 0)
				cout << "Получено сообщение от " << client << " размером " << msg_size << endl;
            else
            {
				cout << "Подключение " << client << " закрыто" << endl;
				close(client);
				client = -1;
				counter -= 1;
			}
		}
	}
	if (client != -1)
		close(client);

	close(sock);
	return 0;
}