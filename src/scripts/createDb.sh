while [[ $# -gt 1 ]]
do
key="$1"

case $key in
	-dbname)
	DBNAME="$2"
	shift  	#past argument
	;;
	-host)
	HOST="$2"
	shift 	#past argument
	;;
	-port)
	PORT="$2"
	shift  	#past argument
	;;
	-user)
	USER="$2"
	shift  	#past argument
	;;
	-password)
	PASSWORD="$2"
	shift  	#past argument
	;;
	*)
		   	#unknown option
	;;
esac
shift		#past argument or value
done

if [ -z ${DBNAME+x} ]
	then
	echo "dbname is required"
	echo "Sample usage: ./createDb.sh -dbname dataengg_test -user skanchi -password tiger -host localhost -port 3306"
	exit
fi
: '
if [ -z ${HOST+x} ]
	then
	echo "host is required"
	echo "Sample usage: ./createDb.sh -dbname dataengg_test -user skanchi -password tiger -host localhost -port 3306"
	exit
fi
if [ -z ${PORT+x} ]
	then
	echo "port is required"
	echo "Sample usage: ./createDb.sh -dbname dataengg_test -user skanchi -password tiger -host localhost -port 3306"
	exit
fi
'
if [ -z ${USER+x} ]
	then
	echo "user is required"
	echo "Sample usage: ./createDb.sh -dbname dataengg_test -user skanchi -password tiger -host localhost -port 3306"
	exit
fi
: '
if [ -z ${PASSWORD+x} ]
	then
	echo "password is required"
	echo "Sample usage: ./createDb.sh -dbname dataengg_test -user skanchi -password tiger -host localhost -port 3306"
	exit
fi
'


echo "Creating database $DBNAME"
psql postgres -U $USER -c "CREATE DATABASE $DBNAME;"

echo "Creating required tables"
psql -U $USER -d $DBNAME -f ./createTables.sql

: '
echo "Populating driver table"
psql -U $USER -d $DBNAME -f ./PopulateDriver.sql

echo "Populating Passenger table"
psql -U $USER -d $DBNAME -f ./PopulatePassenger.sql

echo "Populating Booking table"
psql -U $USER -d $DBNAME -f ./PopulateBooking.sql
'