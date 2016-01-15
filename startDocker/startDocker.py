import time
import getpass
import MySQLdb
import sys
import getopt
import logging
from subprocess import CalledProcessError, check_output, call


def main(argv):

    docker_ip = ""
    docker_img_name = "gnssh/pipwork:v2"
    yarn_host = ""
    yarn_container_id = ""

    try:
        opts, args = getopt.getopt(argv,"hp:i:n:y:",["help","dock erip=","dockerimg=","node=","yarnID="])
    except getopt.GetoptError:
        print 'startDocker.py -p <dockerip> -i <dockerimg> -n <node> -y <yarnID> '
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'startDocker.py -p <dockerip> -i <dockerimg> -n <node> -y <yarnID>'
            sys.exit()
        elif opt in ("-p", "--dockerip"):
            docker_ip = arg
        elif opt in ("-i", "--dockerimg"):
            docker_img_name = arg
        elif opt in ("-n", "--node"):
            yarn_host = arg
        elif opt in ("-y", "--yarnID"):
            yarn_container_id= arg

    print "Host: " + yarn_host
    print "Yarn app ID: "+yarn_container_id

    ip_cmd = "HD_IP="+docker_ip
    docker_id=""
    try:
        docker_id = check_output(["docker", "run", "--privileged=true", "-e", ip_cmd, "-i", "-t", "-d",
                                  docker_img_name]).strip()
        docker_id_short = docker_id[:12]
    except CalledProcessError as e:
        print e.output
    try:
        db = connect_mysql()
        if db is not None:
            cursor = db.cursor()

            add_docker_info = ("INSERT INTO dockerList ( ip, host, dockerID, user, containerID, starttime,  status)"
                             " VALUES (%s, %s, %s, %s, %s,%s, %s)")
            docker_info = (docker_ip, yarn_host, docker_id_short, getpass.getuser(), yarn_container_id,
                           int(time.time()), "RUNNING")

            try:
                cursor.execute(add_docker_info, docker_info)
                db.commit()
            except MySQLdb.DatabaseError:
                db.rollback()

            cursor.close()
        db.close()

    except MySQLdb.MySQLError:
        print "mysql connection failed"

    container_status = "RUNNING"
    while container_status == "RUNNING":
        try:
            db2 = connect_mysql()
            if db2 is not None:
                cursor2 = db2.cursor()
                select_docker_status = ("SELECT status FROM dockerList WHERE dockerID=%s")
                try:
                    cursor2.execute(select_docker_status, docker_id_short)
                    results = cursor2.fetchall()
                    if len(results) > 0:
                        for row in results:
                            container_status = row[0]
                            print container_status
                except MySQLdb.DatabaseError:
                    db2.rollback()

                cursor2.close()
            db2.close()
        except MySQLdb.MySQLError:
            print "mysql connection failed"
        time.sleep(3)
    call(["docker", "stop", docker_id])

    # update ip is available
    try:
        db = connect_mysql()
        if db is not None:
            cursor = db.cursor()
            try:
                cursor.execute("update dockerList set endtime='%s' where dockerID='%s'"% (int(time.time()),docker_id_short))
                cursor.execute("update ipStatus set status=0 where ip='%s'"% docker_ip)
                db.commit()
            except MySQLdb.DatabaseError:
                db.rollback()

            cursor.close()
        db.close()

    except MySQLdb.MySQLError:
        print "mysql connection failed"


def connect_mysql():
    # --- mysql info ---
    mysql_ip = "localhost"
    mysql_user = "root"
    mysql_pwd = "manager1"
    mysql_db = "ipManager"

    try:
        return MySQLdb.connect(mysql_ip, mysql_user, mysql_pwd, mysql_db)
    except MySQLdb.MySQLError:
        logging.error("mysql connection failed")

if __name__ == '__main__':
    # t=random.randrange(1,10)
    # time.sleep(t)
    main(sys.argv[1:])
