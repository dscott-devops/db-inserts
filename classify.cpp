#include <stdlib.h>

#include <chrono>

#include <thread>

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

void classThread(sql::Connection * con, int video_id, string tablename, string items, string *values)
{


  sql::Statement *stmt=NULL;
  sql::ResultSet *res=NULL;

  vector < string > items_vector;
  string query,sqlstring;
  int itemid;
  ofstream errorfile;
  items_vector = split(items, ";");
  try
  {


    stmt = con -> createStatement();


    if (items != "")
    {
  for (std::string str: items_vector) {
      //std::cout << str << std::endl;
      query = "Select id from " + tablename + " where name ='";
      query = query + str;
      query = query + "';";
      //cout << "Select ID" << endl;
      res = stmt -> executeQuery(query);
      while(res -> next())
      {
      itemid = res -> getInt("id");
      sqlstring = "(";
      sqlstring = sqlstring + to_string(int(itemid));
      sqlstring = sqlstring + ",";
      sqlstring = sqlstring + to_string(int(video_id));
      sqlstring = sqlstring + "),";
      *values = *values + sqlstring;
      }
      //cout << "Tag Insert: " << str << endl;
      //stmt -> execute(sqlstring);
      //cout << "Tag Successfully created" << endl;

    } //for tags vector
  }
  }
  catch (sql::SQLException & e) {
    /*
      MySQL Connector/C++ throws three different exceptions:

      - sql::MethodNotImplementedException (derived from sql::SQLException)
      - sql::InvalidArgumentException (derived from sql::SQLException)
      - sql::SQLException (derived from std::runtime_error)
    */
      errorfile.open("errors.txt",fstream::app);
    cout << "# ERR: SQLException in " << __FILE__;
    cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
    /* what() (derived from std::runtime_error) fetches error message */
    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    cout << "SQL String: " << sqlstring << endl;

    errorfile << "# ERR: SQLException in " << __FILE__;
    errorfile << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
    /* what() (derived from std::runtime_error) fetches error message */
    errorfile << "# ERR: " << e.what();
    errorfile << " (MySQL error code: " << e.getErrorCode();
    errorfile << ", SQLState: " << e.getSQLState() << " )" << endl;
    errorfile << "SQL String: " << sqlstring << endl;
    errorfile.close();



  }


delete res;
delete stmt;





}



int main() {

  string embed;
  string ifilename;
  string ofilename;
  string dirname;
  string thumb;
  string thumbs;
  string title;
  string tags;
  string tagssql, catssql, pornssql;
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
  string tagvalues;
  string catvalues;
  string pornstarvalues;
  string sqlstring;
  string query;
  int count;
  int videoid;
  int insertcount;
  int i;
  double lastid;
  double tagid;
  double catid;
  char buffer [100];
  std::string s;
  sql::Driver * driver;
  sql::Connection * con;
  sql::ResultSet * res;
  sql::Statement * stmt;

  ofstream outputfile;


  vector < string > tags_vector;
  vector < string > cats_vector;
  vector < string > pornstars_vector;
  vector < string > files_vector;


  outputfile.open("results.txt");



  driver = get_driver_instance();
  con = driver -> connect("tcp://127.0.0.1:3306", "mysqluser", "DaBus099");
  /* Connect to the MySQL test database */
  con -> setSchema("videos");




  stmt = con -> createStatement();








  count = 0;
  insertcount = 0;
  tagssql = "Insert into tag_videos (tag_id, video_id) Values ";
  catssql = "Insert into category_videos (category_id, video_id) Values ";
  pornssql = "Insert into pornstar_videos (pornstar_id, video_id) Values ";
  sqlstring = "Select id, pornstars, tags, category from videos LIMIT 5000 OFFSET ";
  try
  {
    count = 0;
  for(i=0;i < 3300000;i+=5000)
  {
    query = sqlstring + to_string(i) + ";";
    cout << query << endl;
    res = stmt->executeQuery(query);
    while(res->next())
     {
       count++;
       videoid = res -> getInt("id");
       pornstars = res -> getString("pornstars");
       category = res -> getString("category");


       tags = res -> getString("tags");
    //   cats_vector = split(category, ";");
    //   pornstars_vector = split(pornstars, ";");

        classThread(con,videoid, "tags", tags,&tagvalues);
        classThread(con,videoid, "categories", category,&catvalues);
        classThread(con, videoid, "pornstars", pornstars,&pornstarvalues);



        cout << "Records Completed: " << count << endl;


    //
    //
    //
    }
    tagvalues = tagvalues.substr(0, tagvalues.length() - 1);
    catvalues = catvalues.substr(0, catvalues.length() - 1);
    pornstarvalues = pornstarvalues.substr(0, pornstarvalues.length() - 1);

    query = tagssql + tagvalues + ";";
    outputfile << query << "\n";
    query = catssql + catvalues + ";";
    outputfile << query << "\n";
    query = pornssql + pornstarvalues + ";";
    outputfile << query << "\n";
    tagvalues = "";
    catvalues = "";
    pornstarvalues = "";











    //stmt -> execute(sqlstring);
    cout << "Records completed: " << count << endl;


  }

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
    cout << "SQL String: " << sqlstring << endl;


  }














outputfile.close();



return (0);
}
