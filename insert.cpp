#include <stdlib.h>

#include <chrono>

#include <iostream>

#include <fstream>

#include <stdio.h>

#include <dirent.h>

#include <filesystem>

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

enum sites {pornhub, xvideos, xtube, South};

int main(int argc, char ** argv) {

  string embed;
  string ifilename;
  string ofilename;
  string dirname;
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
  string site;
  string delim;
  int count;
  int insertcount;
  int fieldlen;
  int sitenum;
  int titlenum, pornstarnum, i;
  string *names;
  string *data;
  char buffer [100];
  std::string s;
  sql::Driver * driver;
  sql::Connection * con;
  sql::ResultSet * res;
  //sql::PreparedStatement * pstmt;
  sql::Statement * stmt;
  ifstream inputfile;
  ofstream outputfile;
  ofstream errorfile;
  ofstream catfile;
  ofstream tagfile;

  vector < string > duration_vector;
  vector < string > cats_vector;
  vector < string > pornstars_vector;
  vector < string > files_vector;



  if (argc == 1) {
    inputfile.open("videos.csv");
    outputfile.open("results.txt");
    errorfile.open("errors.txt");
    catfile.open("cats.txt");
    tagfile.open("tags.txt");
  } else if (argc == 4) {
    dirname = argv[1];
    ofilename = argv[2];
    sitenum = atoi(argv[3]);
    outputfile.open(ofilename);
    errorfile.open("errors.txt");
    catfile.open("cats.txt");
    tagfile.open("tags.txt");

  } else {
    cout << "USAGE:";
    return (2);
  }

  switch(sitenum) {
   case 1  :
   insertsql = "Insert into videos (site, embed, thumb, title, tags, category,  pornstars, \
      duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) VALUES ";
   fieldlen = 12;
   names = new string[fieldlen];
   data = new string[fieldlen];
   site = "pornhub.com";
   titlenum = 2;
   pornstarnum = 5;
   delim = "|";
   names[0] = "embed";
   names[1] = "thumb";
   names[2] = "title";
   names[3] = "tags";
   names[4] = "category";
   names[5] = "pornstars";
   names[6] = "duration";
   names[7] = "views";
   names[8] = "likes";
   names[9] = "dislikes";
   names[10] = "bigthumb";
   names[11] = "bigthumbs";
   std::regex rgx(".*src=\"(.*?)\"");
   std::smatch match;



      break; //optional
   case 2  :
   insertsql = "Insert into videos (site, weblink, title, duration, thumb, embed, tags, pornstars, video_id,category) VALUES ";
   fieldlen = 10;
   names = new string[fieldlen];
   data = new string[fieldlen];
   site = "xvideos.com";
   titlenum = 1;
   pornstarnum = 7;
   names[0] = "weblink";
   names[1] = "title";
   names[2] = "title";
   names[3] = "duration";
   names[4] = "thumb";
   names[5] = "embed";
   names[6] = "tags";
   names[7] = "pornstars";
   names[8] = "video_id";
   names[9] = "category";

      break; //optional

}



  /* Create a connection */
  driver = get_driver_instance();
  con = driver -> connect("tcp://127.0.0.1:3306", "mysqluser", "DaBus099");
  /* Connect to the MySQL test database */
  con -> setSchema("videos");


  //pstmt = con -> prepareStatement("Insert into videos (site, embed, thumb, title, tags, category,  pornstars, \
        duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) \
        VALUES ('pornhub.com', ?,?,?,?,?,?,?,?,?,?,?,?,?)");

  stmt = con -> createStatement();
  sql::mysql::MySQL_Connection * mysql_conn = dynamic_cast<sql::mysql::MySQL_Connection*>(con);
  count = 0;
  insertcount = 0;



  DIR *folder;
  struct dirent *entry;


  folder = opendir(argv[1]);
  if(folder == NULL)
  {
      perror("Unable to read directory");
      return(1);
  }




  while( (entry=readdir(folder)) )
  {
    sprintf(buffer,"%s",entry->d_name);
  if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
    {

      files_vector.push_back(buffer);

    }
  }



  for (std::string str: files_vector) {
    cout << "Files: " << str << endl;

      ifilename = dirname + "/" + str;
      cout << "Filename: " << ifilename << endl;
      inputfile.open(ifilename);

      if (!inputfile.is_open()) {
        std::cout << "Error: File Open" << '\n';
        return (3);
      }

    while (inputfile.good()) {

    try {
      count++;
      insertcount++;

      for(i=0;i<(fieldlen - 2);i++)
      {
        getline(inputfile, data[i], delim);
      }
      getline(inputfile, data[fieldlen - 1], '\n');

      data[titlenum] = mysql_conn->escapeString( data[titlenum] );
      data[pornstarnum] = mysql_conn->escapeString( data[pornstarnum] );

      switch(sitenum)
      {
        case 1:
        if (std::regex_search(data[0], match, rgx)) {
          //std::cout << "match: " << match[1] << '\n';
        }
        temp = match[1];
        video_id = temp.substr(temp.find_last_of("/\\") + 1);
        value = "('" + site + "'," + data[0] + "','";
        value = value + data[1] + "','";
        value = value + data[2] + "','";
        value = value + data[3] + "','";
        value = value + data[4] + "','";
        value = value + data[5] + "','";
        value = value + data[6] + "','";
        value = value + data[7] + "','";
        value = value + data[8] + "','";
        value = value + data[9] + "','";
        value = value + data[10] + "','";
        value = value + data[11] + "','";
        value = value + video_id + "')," + "\n";

        //pstmt = con -> prepareStatement("Insert into videos \
        (site, embed, thumb, title, tags, category,  pornstars, \
              duration, views, likes, dislikes, bigthumb, bigthumbs,video_id) \
              VALUES ('pornhub.com', ?,?,?,?,?,?,?,?,?,?,?,?,?)");

        break;
        case 2:
        for(i=0;i<(fieldlen - 2);i++)
        {
          getline(inputfile, data[i], delim);
        }
        getline(inputfile, data[fieldlen - 1], '\n');

        names[0] = "weblink";
        names[1] = "title";
        names[2] = "title";
        names[3] = "duration";
        names[4] = "thumb";
        names[5] = "embed";
        names[6] = "tags";
        names[7] = "pornstars";
        names[8] = "video_id";
        names[9] = "category";

        duration_vector = split(data[3]," ");

        data[3] = duration_vector.front();

        value = "('" + site + "'," + data[0] + "','";
        value = value + data[1] + "','";
        value = value + data[2] + "','";
        value = value + data[3] + "','";
        value = value + data[4] + "','";
        value = value + data[5] + "','";
        value = value + data[6] + "','";
        value = value + data[7] + "','";
        value = value + data[8] + "','";
        value = value + data[9] + "')," + "\n";

        break;
      }







      if (values == "") {
        values = value;
      } else {
        values = values + value;
      }

      if (insertcount == 1000) {
        values = values.substr(0, values.length() - 2);
        sqlstring = insertsql + values + ";";
        outputfile << sqlstring << "\n";
        //stmt -> execute(sqlstring);
        cout << "Records completed: " << count << endl;


        insertcount = 0;
        values = "";
      }

      //pstmt -> executeUpdate();
      // sqlstring = "SELECT LAST_INSERT_ID() AS id;";
      // res = stmt -> executeQuery(sqlstring);
      // res -> next();
      // lastid = res -> getInt("id");
      //
      // sqlstring = insertsql + " " + value;
      //stmt->execute(sqlstring);
      // if (!lastid == 0) {
      //   tags_vector = split(tags, ";");
      //   for (std::string str: tags_vector) {
      //     //std::cout << str << std::endl;
      //     query = "Select id from tags where name ='";
      //     query = query + str;
      //     query = query + "';";
      //     //cout << "Select ID" << endl;
      //     res = stmt -> executeQuery(query);
      //     res -> next();
      //     tagid = res -> getInt("id");
      //     sqlstring = "Insert into tag_videos (tag_id, video_id) Values (";
      //     sqlstring = sqlstring + to_string(int(tagid));
      //     sqlstring = sqlstring + ",";
      //     sqlstring = sqlstring + to_string(int(lastid));
      //     sqlstring = sqlstring + ");";
      //     //cout << "Tag Insert: " << str << endl;
      //     stmt -> execute(sqlstring);
      //     //cout << "Tag Successfully created" << endl;
      //
      //   } //for tags vector
      //
      //   cats_vector = split(category, ";");
      //
      //   for (std::string str: cats_vector) {
      //     query = "Select id from categories where name ='";
      //     query = query + str;
      //     query = query + "';";
      //     //cout << "Select ID" << endl;
      //     res = stmt -> executeQuery(query);
      //     res -> next();
      //     catid = res -> getInt("id");
      //     sqlstring = "Insert into category_videos (category_id, video_id) Values (";
      //     sqlstring = sqlstring + to_string(int(catid));
      //     sqlstring = sqlstring + ",";
      //     sqlstring = sqlstring + to_string(int(lastid));
      //     sqlstring = sqlstring + ");";
      //     //cout << "Tag Insert: " << str << endl;
      //     stmt -> execute(sqlstring);
      //     //cout << "Tag Successfully created" << endl;
      //
      //   }
      //
      //   pornstars_vector = split(pornstars,";");
      //   for (std::string str: pornstars_vector) {
      //     query = "Select id from pornstars where name ='";
      //     query = query + str;
      //     query = query + "';";
      //     //cout << "Select ID" << endl;
      //     res = stmt -> executeQuery(query);
      //     res -> next();
      //     catid = res -> getInt("id");
      //     sqlstring = "Insert into pornstar_videos (pornstar_id, video_id) Values (";
      //     sqlstring = sqlstring + to_string(int(catid));
      //     sqlstring = sqlstring + ",";
      //     sqlstring = sqlstring + to_string(int(lastid));
      //     sqlstring = sqlstring + ");";
      //     //cout << "Tag Insert: " << str << endl;
      //     stmt -> execute(sqlstring);
      //     //cout << "Tag Successfully created" << endl;
      //
      //   }
      //
      // } //if !lastid
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



  }  //end while

  std::cout << "Records found: " << count << '\n';
  inputfile.close();





} //for files_vector
outputfile.close();
errorfile.close();
catfile.close();
tagfile.close();


return (0);
}
