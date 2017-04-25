/**
  *数据库连接池
  *2017/04/24
  **/
#ifndef MYSQL_CONN_POOL_H
#define MYSQL_CONN_POOL_H

#include <mysql_connection.h>
#include <mysql_driver.h>
#include <cppconn/exception.h>
//#include <cppconn/resultset.h>
//#include <cppconn/statement.h>
//#include <cppconn/prepared_statement.h>

#include <pthread.h>
#include <list>
    
using namespace std;


class ConnPool{
    private:
        list<sql::Connection *> conns;//连接队列
        int curSize;//当前队列中路连接数目
        int maxSize;//最大连接数目
        sql::ConnectOptionsMap connectionProperties;
        std::string db_name;
        pthread_mutex_t lock;//连接队列互斥锁
        static ConnPool * connPool;
//        sql::Driver * driver;//mysql connector C++ driver
        sql::mysql::MySQL_Driver *driver;   //do not explicitly free driver, the connector object. Connector/C++ takes care of freeing that
        sql::Connection * CreateConnection();//创建一个连接
        void TerminateConnection(sql::Connection * conn);//终止一个连接
        void Init(int initialSize);//初始化连接池
        void Destroy();//销毁连接池
    protected:
        ConnPool(string host,string user,string password,string dbname,int maxSize,int socketTimeout, int connectTimeout);
    public:
        ~ConnPool();
        sql::Connection * GetConnection();//获取一个连接
        void ReleaseConnection(sql::Connection * conn);//释放一个连接
        sql::Connection * GetConnectionTry(int maxNum);//GetConnection的加强版，maxNum代表重试次数
        static ConnPool * GetInstance(string host,string user,string password,string dbname,int maxSize,int socketTimeout, int connectTimeout);//获取一个ConnPool对象实例
};

#endif
