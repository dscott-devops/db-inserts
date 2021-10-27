#include <iostream>
#include <vector>
#include <string>
#include <filesystem>

using std::cout; using std::cin;
using std::endl; using std::string;
using std::filesystem::recursive_directory_iterator;

int main() {
    string path = "/home/dscott/forex";

    for (const auto & file : recursive_directory_iterator(path))
        cout << file.path() << endl;

    return EXIT_SUCCESS;
}
