// Создает копии файлов, которые являются ссылками. Облегчает тестирование основного скрипта

#include <iostream>
#include <filesystem>
#include <vector>

// &Output: revert.exe

using namespace std;

void restore_hardlinks(const filesystem::path& directory) 
{
    try 
    {
        for (const auto& entry : filesystem::recursive_directory_iterator(directory)) {
            if (entry.is_regular_file() && filesystem::hard_link_count(entry.path()) > 1) 
            {
                filesystem::path filepath = entry.path();
                filesystem::path temp_path = filepath.string() + ".restoring";
                
                // Создаем независимую копию
                filesystem::copy_file(filepath, temp_path, filesystem::copy_options::overwrite_existing);
                
                // Удаляем жесткую ссылку и заменяем на копию
                filesystem::remove(filepath);
                filesystem::rename(temp_path, filepath);
                
                cout << "Восстановлен: " << filepath << endl;
            }
        }
    } catch (const filesystem::filesystem_error& e) {
        cerr << "Ошибка: " << e.what() << endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) 
    {
        cerr << "Ошибка: программа может принимать единственный аргумет: директория для работы" << endl;
        return 1;
    }
    
    restore_hardlinks(argv[1]);
    return 0;
}