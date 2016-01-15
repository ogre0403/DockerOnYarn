import MySQLdb
import sys
import logging

def main(argv):

    db = connect_mysql()
    if db is not None:
        cursor = db.cursor()

        add_dockerInfo= ("INSERT INTO dockerList ( ip, host)"
                                 " VALUES (%s, %s)")
        docker_info = ("192.168.1.1","TCPOC")

        try:
            cursor.execute(add_dockerInfo, docker_info)
            db.commit()
        except:
            db.rollback()
    db.close()

def connect_mysql():
    ##### mysql info ######
    MysqlIP = "140.110.141.62"
    MysqlUser = "root"
    MysqlPwd = "1234"
    MysqlDb = "ipManager"

    try:
        return MySQLdb.connect(MysqlIP,MysqlUser,MysqlPwd,MysqlDb)
    except:
        logging.error("mysql connection failed")

if __name__ == '__main__':
    #t=random.randrange(1,10)
    #time.sleep(t)
    main(sys.argv[1:])
