#!/bin/sh
# Первый аргумент вызова - название docker volume
# Второй аргумент - количество одновременно запущенных контейнеров

vol_name=$1;  # Наименование тома
container_num=$2; # Количество контейнеров

exit_handler()
{
    echo "Остановка контейнеров"
    return_code=$?
    trap - EXIT INT HUP QUIT TERM
    i=1
    while [ $i -le $container_num ]; do
        docker stop "container_$i"
        i=$((i+1))
    done
    docker rmi lab2_unix
    exit $return_code
}

trap exit_handler INT HUP QUIT TERM EXIT

docker build -t lab2_unix . || exit 1
i=1;
while [ $i -le $container_num ]; do
    docker run -d --rm --name "container_$i" --volume $vol_name:/app/shared lab2_unix || exit 2
    i=$((i+1));
done

echo "Контенеры в работе"

while :; do
{
    sleep 200
}
done