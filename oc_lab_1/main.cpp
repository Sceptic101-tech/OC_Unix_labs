#include <iostream>
#include <pthread.h>
#include <unistd.h>
using namespace std;

// &Output: run.me

pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int ready = 0;

void* provider_worker(void*)
{
    while (true)
    {
        sleep(1);
        pthread_mutex_lock(&mutex);
        if (ready == 1)
        {
            pthread_mutex_unlock(&mutex);
            continue;
        }
        ready = 1;
        pthread_cond_signal(&condition);
        cout << "Поставщик отработал" << endl;
        pthread_mutex_unlock(&mutex);
    }
    return nullptr;
}

void* consumer_worker(void*)
{
    while (true)
    {
        pthread_mutex_lock(&mutex);
        while (ready == 0)
        {
            cout << "Потребитель ожидает данные" << endl;
            pthread_cond_wait(&condition, &mutex);
        }
        cout << "Потребитель получил данные" << endl << endl;
        ready = 0;
        pthread_mutex_unlock(&mutex);
    }
    return nullptr;
}

int main(int argc, char* argv[])
{
    pthread_t provider, consumer;
    pthread_create(&consumer, nullptr, consumer_worker, nullptr);
    pthread_create(&provider, nullptr, provider_worker, nullptr);
    pthread_join(consumer, nullptr);
    pthread_join(provider, nullptr);
    return 0;
}