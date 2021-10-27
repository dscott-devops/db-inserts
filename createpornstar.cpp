#include <stdlib.h>

#include <chrono>

#include <iostream>

#include <fstream>

#include <stdio.h>

#include <dirent.h>

#include <regex>

#include <boost/algorithm/string/replace.hpp>

#include "mysql_connection.h"


#include <cppconn/driver.h>

#include <cppconn/exception.h>

#include <cppconn/resultset.h>

#include <cppconn/statement.h>

#include <cppconn/prepared_statement.h>

using namespace std;

std::vector < std::string > split(const std::string & text,
  const std::string & delims) {
  std::vector < std::string > tokens;
  std::size_t start = text.find_first_not_of(delims), end = 0;

  while ((end = text.find_first_of(delims, start)) != std::string::npos) {
    tokens.push_back(text.substr(start, end - start));
    start = text.find_first_not_of(delims, end);
  }
  if (start != std::string::npos)
    tokens.push_back(text.substr(start));

  return tokens;
}

int main(int argc, char ** argv) {

  string embed;
  string ifilename;
  string ofilename;
  string thumb;
  string thumbs;
  string title;
  string tags;
  string category;
  string pornstars;
  string duration;
  string views;
  string likes;
  string dislikes;
  string bigthumb;
  string bigthumbs;
  string video_id;
  string temp;
  string insertsql;
  string values;
  string value;
  string sqlstring;
  string query;
  int count;
  int insertcount;
  double lastid;
  double tagid;
  double catid;
  std::string s;
  sql::Driver * driver;
  sql::Connection * con;
  sql::ResultSet * res;
  sql::PreparedStatement * pstmt;
  sql::Statement * stmt;
  ifstream inputfile;
  ofstream outputfile;
  ofstream errorfile;
  ofstream catfile;
  ofstream tagfile;
  vector < string > tags_vector;
  vector < string > cats_vector;
  vector < string > pornstars_vector;

  if (argc == 1) {
    inputfile.open("videos.csv");
    outputfile.open("results.txt");
    errorfile.open("errors.txt");
    catfile.open("pornstars.txt");
    tagfile.open("tags.txt");
  } else if (argc == 3) {
    ifilename = argv[1];
    ofilename = argv[2];
    inputfile.open(ifilename);
    outputfile.open(ofilename);
    errorfile.open("errors.txt");
    catfile.open("pornstars.txt");
    tagfile.open("tags.txt");

  } else {
    cout << "USAGE:";
    return (2);
  }

  if (!inputfile.is_open()) {
    std::cout << "Error: File Open" << '\n';
    return (3);
  }

  /* Create a connection */
  driver = get_driver_instance();
  con = driver -> connect("tcp://127.0.0.1:3306", "mysqluser", "DaBus099");
  /* Connect to the MySQL test database */
  con -> setSchema("videos");
  std::regex rgx(".*src=\"(.*?)\"");
  std::smatch match;

  pstmt = con -> prepareStatement("Insert into videos (site, embed, thumb, title, tags, category,  pornstars, \
        duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) \
        VALUES ('pornhub.com', ?,?,?,?,?,?,?,?,?,?,?,?,?)");

  stmt = con -> createStatement();
  count = 0;
  insertcount = 0;
  //pstmt = con->prepareStatement("INSERT INTO videos_test(,bid,ask) VALUES (?,?,?)");
  //pstmt = con->prepareStatement("INSERT into videos (site, embed, thumb, title, tags, category,pornstars,duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
  insertsql = "Insert into videos_test (site, embed, thumb, title, tags, category,  pornstars,  duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) VALUES ";
  while (inputfile.good()) {

    try {
      count++;
      insertcount++;

      getline(inputfile, embed, '|');
      getline(inputfile, thumb, '|');
      getline(inputfile, thumbs, '|');
      getline(inputfile, title, '|');
      getline(inputfile, tags, '|');
      getline(inputfile, category, '|');
      getline(inputfile, pornstars, '|');
      getline(inputfile, duration, '|');
      getline(inputfile, views, '|');
      getline(inputfile, likes, '|');
      getline(inputfile, dislikes, '|');
      getline(inputfile, bigthumb, '|');
      getline(inputfile, bigthumbs, '\n');

      // if (views == "") {
      //   views = "0";
      // }
      // if (likes == "") {
      //   likes = "0";
      // }
      // if (dislikes == "") {
      //   dislikes = "0";
      // }
      // if (duration == "") {
      //   duration = "0";
      // }
      //
      // if (std::regex_search(embed, match, rgx)) {
      //   //std::cout << "match: " << match[1] << '\n';
      // }
      //
      // temp = match[1];
      // video_id = temp.substr(temp.find_last_of("/\\") + 1);
      // //cout << "This is video id: " << video_id << endl;
      // //cout << "This is title: " << title << endl;
      // //cout << "This is duration: " << duration << endl;
      // value = "('pornhub.com','" + embed + "','";
      // value = value + thumb + "','";
      // value = value + title + "','";
      // value = value + tags + "','";
      // value = value + category + "','";
      // value = value + pornstars + "','";
      // value = value + duration + "','";
      // value = value + views + "','";
      // value = value + likes + "','";
      // value = value + dislikes + "','";
      // value = value + bigthumb + "','";
      // value = value + bigthumbs + "','";
      // value = value + video_id + "');" + "\n";
      //
      // pstmt -> setString(1, embed);
      // pstmt -> setString(2, thumb);
      // pstmt -> setString(3, title);
      // pstmt -> setString(4, tags);
      // pstmt -> setString(5, category);
      // pstmt -> setString(6, pornstars);
      // pstmt -> setString(7, duration);
      // pstmt -> setString(8, views);
      // pstmt -> setString(9, likes);
      // pstmt -> setString(10, dislikes);
      // pstmt -> setString(11, bigthumb);
      // pstmt -> setString(12, bigthumbs);
      // pstmt -> setString(13, video_id);
      //
      // if (values == "") {
      //   values = value;
      // } else {
      //   values = values + value;
      // }
      //
      // if (insertcount == 10) {
      //   values = values.substr(0, values.length() - 2);
      //   //outputfile << insertsql  << "\n" << values << ";" << endl;
      //   insertcount = 0;
      //   values = "";
      // }
      // lastid = 0;
      // pstmt -> executeUpdate();
      // sqlstring = "SELECT LAST_INSERT_ID() AS id;";
      // res = stmt -> executeQuery(sqlstring);
      // res -> next();
      // lastid = res -> getInt("id");
      //
      // sqlstring = insertsql + " " + value;
      //stmt->execute(sqlstring);
      pornstars_vector = split(pornstars,";");
      for (std::string str: pornstars_vector) {
        //std::cout << str << std::endl;

        catfile << str << "\n";

        //cout << "Select ID" << endl;

        // sqlstring = "Insert into tag_videos (tag_id, video_id) Values (";
        // sqlstring = sqlstring + to_string(int(tagid));
        // sqlstring = sqlstring + ",";
        // sqlstring = sqlstring + to_string(int(lastid));
        // sqlstring = sqlstring + ");";
        //cout << "Tag Insert: " << str << endl;
        //stmt -> execute(sqlstring);
        //cout << "Tag Successfully created" << endl;

      } //for tags vector

      if (!lastid == 0) {
        // tags_vector = split(tags, ";");
        // for (std::string str: tags_vector) {
        //   //std::cout << str << std::endl;
        //   query = "Select id from tags where name ='";
        //   query = query + str;
        //   query = query + "';";
        //   //cout << "Select ID" << endl;
        //   res = stmt -> executeQuery(query);
        //   res -> next();
        //   tagid = res -> getInt("id");
        //   sqlstring = "Insert into tag_videos (tag_id, video_id) Values (";
        //   sqlstring = sqlstring + to_string(int(tagid));
        //   sqlstring = sqlstring + ",";
        //   sqlstring = sqlstring + to_string(int(lastid));
        //   sqlstring = sqlstring + ");";
        //   //cout << "Tag Insert: " << str << endl;
        //   stmt -> execute(sqlstring);
        //   //cout << "Tag Successfully created" << endl;
        //
        // } //for tags vector
        //
        // cats_vector = split(category, ";");
        //
        // for (std::string str: cats_vector) {
        //   query = "Select id from categories where name ='";
        //   query = query + str;
        //   query = query + "';";
        //   //cout << "Select ID" << endl;
        //   res = stmt -> executeQuery(query);
        //   res -> next();
        //   catid = res -> getInt("id");
        //   sqlstring = "Insert into category_videos (category_id, video_id) Values (";
        //   sqlstring = sqlstring + to_string(int(catid));
        //   sqlstring = sqlstring + ",";
        //   sqlstring = sqlstring + to_string(int(lastid));
        //   sqlstring = sqlstring + ");";
        //   //cout << "Tag Insert: " << str << endl;
        //   stmt -> execute(sqlstring);
        //   //cout << "Tag Successfully created" << endl;
        //
        // }


        // for (std::string str: pornstars_vector) {
        //   query = "Select id from pornstars where name ='";
        //   query = query + str;
        //   query = query + "';";
        //   //cout << "Select ID" << endl;
        //   res = stmt -> executeQuery(query);
        //   res -> next();
        //   catid = res -> getInt("id");
        //   sqlstring = "Insert into pornstar_videos (pornstar_id, video_id) Values (";
        //   sqlstring = sqlstring + to_string(int(catid));
        //   sqlstring = sqlstring + ",";
        //   sqlstring = sqlstring + to_string(int(lastid));
        //   sqlstring = sqlstring + ");";
        //   //cout << "Tag Insert: " << str << endl;
        //   stmt -> execute(sqlstring);
        //   //cout << "Tag Successfully created" << endl;
        //
        // }

      } //if !lastid
    } catch (sql::SQLException & e) {
      /*
        MySQL Connector/C++ throws three different exceptions:

        - sql::MethodNotImplementedException (derived from sql::SQLException)
        - sql::InvalidArgumentException (derived from sql::SQLException)
        - sql::SQLException (derived from std::runtime_error)
      */
      cout << "# ERR: SQLException in " << __FILE__;
      cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
      /* what() (derived from std::runtime_error) fetches error message */
      cout << "# ERR: " << e.what();
      cout << " (MySQL error code: " << e.getErrorCode();
      cout << ", SQLState: " << e.getSQLState() << " )" << endl;

      errorfile << "# ERR: " << e.what() << " (MySQL error code: " << e.getErrorCode() << ", SQLState: " << e.getSQLState() << " )" << endl;
      errorfile << "SQL String: " << sqlstring << endl;
      errorfile << "Query String: " << query << endl;
      errorfile << "Embed: " << embed << "\n";
      errorfile << "Thumb: " << thumb << "\n";
      errorfile << "Title: " << title << "\n";
      errorfile << "Tags: " << tags << "\n";
      errorfile << "Category: " << category << "\n";
      errorfile << "Pornstars: " << pornstars << "\n";
      errorfile << "Duration: " << duration << "\n";
      errorfile << "Views: " << views << "\n";
      errorfile << "Likes: " << likes << "\n";
      errorfile << "Dislikes: " << dislikes << "\n";
      errorfile << "BigThumb: " << bigthumb << "\n";
      errorfile << "BigThumbs: " << bigthumbs << "\n";
      errorfile << "Video ID: " << video_id << "\n\n\n";

    }

      cout << "Records completed: " << count << endl;

  }  //end while

  std::cout << "Records found: " << count << '\n';
  inputfile.close();
  outputfile.close();
  errorfile.close();
  catfile.close();
  tagfile.close();

  return (0);

}
