#!/bin/sh
# Компиляция C++ программы
# Имя исполняемого файла берется из исходного кода по паттерну
# Первый аргумент вызова - имя исходного файла
# Второй аргумент - паттерн

exit_handler()
{
    return_code=$?
    trap - EXIT # Сбрасываем на поведение по умолчанию
    rm -rf "$temp_dir"
    exit $return_code
}
trap exit_handler EXIT TERM INT QUIT

#Проверка существования файла
if [ ! -f "$1" ]; then
    echo "Error: File \"$1\" not exists" >&2
    exit 1
else
    file="$1"
    echo "OK: File \"$1\" exists"
fi

# Извлечение имени файла из скрипта
regular=".*$2[[:space:]]*\([^[:space:]]\{1,\}\)"
compiled_filename=""
while read line; do
    # Используем expr для проверки регулярного выражения
    if expr "$line" : "$regular"; then
        compiled_filename=$(expr "$line" : "$regular")
        echo "Match found. Compiled name: $compiled_filename"
        break
    fi
done < "$file"

# проверка, нашлось ли совпадение
if [ -z "$compiled_filename" ]; then
    echo "Error: \"$2\" pattern not found in a code" >&2
    exit 2
fi

# Создание временной директории и компиляция
origin_dir=$(pwd)
temp_dir=$(mktemp -d) || exit 3

if ! g++ "$1" -o "$temp_dir/$compiled_filename"; then
    echo "Error: Compilation failed" >&2
    exit 4
fi

cp "$temp_dir/$compiled_filename" "$origin_dir" || exit 5

exit 0