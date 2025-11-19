// Создает жесткие ссылки для файлов, хэш которых совпал
// &Output: run.me
#include <iostream>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <system_error>
#include <stdexcept>

#include "cryptlib.h"
#include "sha.h"
#include "hex.h"
#include "filters.h"
#include "files.h"

using namespace std;
using namespace CryptoPP;

// Считывает содержимое файла в переменную типа string
string read_file_content(const filesystem::path& filepath)
{
    string file_representation;
 
    std::ifstream file(filepath.string());
    if (!file.is_open())
    {   
        throw "Ошибка: Не удалось открыть файл по пути " + filepath.string();
    }
    std::ostringstream oss;
    oss << file.rdbuf();
    file_representation = oss.str();
    file.close();
    return file_representation;
}

// Вычисляет хэш строки при помощи алгоритма SHA-1
string compute_string_hash(const string& file_representation)
{
    string digest;
    string result;
    try
    {
        HexEncoder encoder(new StringSink(result));
        CryptoPP::SHA1 hash;
        hash.Update((const CryptoPP::byte*)file_representation.data(), file_representation.size());
        digest.resize(hash.DigestSize());
        hash.Final((CryptoPP::byte*)&digest[0]);

    StringSource(digest, true, new Redirector(encoder));
    }
    catch(const std::exception& e)
    {
        cerr << "Ошибка: не удалось вычислить хэш" << e.what();
    }
    return result;
}

// Создание жестких ссылок
void create_hard_links(const vector<filesystem::path>& files)
{
    if (files.size() < 2)
        throw "Ошибка: Для создания жестких ссылок необходимо не менее двух файлов";
    
    const filesystem::path& target = files.at(0);
    
    for (int i = 1; i < files.size(); i++) 
    {
        const filesystem::path& source = files.at(i);
        
        try 
        {
            filesystem::remove(source);
            filesystem::create_hard_link(target, source);
            cout << "Создана жесткая ссылка: " << source << " ~ " << target << endl;
        } catch (const exception& execpt)
        {
            cerr << "Ошибка при создании жесткой ссылки для " << source << ": " << execpt.what() << endl;
        }
    }
}

// Сканирование массива и создание ссылок для файлов, хэш которых совпал
void handle_collisions(const unordered_map<string, vector<filesystem::path>>& files_hash)
{
    for (const std::pair<string, vector<filesystem::path>>& hashed_group : files_hash)
    {
        if (hashed_group.second.size() > 1)
        {
            cout << "Обнаружена коллизия между " << hashed_group.second.size() << " файлами:" << endl;
            for(auto& el : hashed_group.second)
                cout << el << endl;
            
            cout << endl;
            create_hard_links(hashed_group.second);
        }
    }
}

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        cerr << "Ошибка: программа может принимать единственный аргумет: директория для работы" << endl;
        return 1;
    }
    
    filesystem::path origin_directory = argv[1];
    
    if (!filesystem::exists(origin_directory) || !filesystem::is_directory(origin_directory))
    {
        cerr << "Ошибка: " << origin_directory << " не является директорией или не существует" << endl;
        return 2;
    }
    
    unordered_map<string, vector<filesystem::path>> files_hash; // Логика такая: для каждого значения хэша приписываем массив файлов.
                                                                // В идеале когда нет коллизий, массив имеет единичную длину
    int file_counter = 0; // Счетчик обработанных файлов
    string file_hash;
    
    try
    {
        // Рекурсивный обход директорий
        for (auto& entry : filesystem::recursive_directory_iterator(origin_directory))
        {
            if (entry.is_regular_file())
            {
                // Не учитываем симлинки
                if (entry.is_symlink())
                    continue;

                filesystem::path filepath = entry.path();

                file_counter++;

                file_hash = read_file_content(filepath);
                
                std::string file_hash = compute_string_hash(read_file_content(filepath));
                
                if (!file_hash.empty())
                {
                    files_hash[file_hash].push_back(filepath);
                }
            }
        }
        cout << "Всего найдено " << file_counter << "файлов" << endl;
        handle_collisions(files_hash);
        
    }
    catch (exception& e)
    {
        cerr << "Ошибка: " << e.what() << endl;
        return 1;
    }
    
    return 0;
}