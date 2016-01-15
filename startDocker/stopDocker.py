import MySQLdb
import logging
import getopt
import sys


def main(argv):

    user = ""

    try:
        opts, args = getopt.getopt(argv, "hu:", ["help", "user="])
    except getopt.GetoptError:
        print 'startDocker.py -u <user>'
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'stopDocker.py -u <user>'
            sys.exit()
        elif opt in ("-u", "--user"):
            user = arg

    # update ip is available
    try:
        db = connect_mysql()
        if db is not None:
            cursor = db.cursor()
            try:
                cursor.execute("update dockerList set status='STOPPING' where user='%s'" % user)

                db.commit()
            except MySQLdb.MySQLError as e:
                print e
                db.rollback()

            cursor.close()
        db.close()

    except MySQLdb.connection.error as e:
        print "mysql connection failed "+e


def connect_mysql():

    # --- mysql info ---
    mysql_ip = "140.110.141.62"
    mysql_user = "root"
    mysql_pwd = "1234"
    mysql_db = "ipManager"

    try:
        return MySQLdb.connect(mysql_ip, mysql_user, mysql_pwd, mysql_db)
    except MySQLdb.connection.error as e:
        logging.error("mysql connection failed " + e)

if __name__ == '__main__':
    # t=random.randrange(1,10)
    # time.sleep(t)
    main(sys.argv[1:])
