import happybase
import datetime
import uuid

MAIN_HOST="quickstart.cloudera"
HBASE_TABLE = "socialmedia"
HBASE_HOST = MAIN_HOST

def create_table(delete_table=False):
    connection = happybase.Connection(host=HBASE_HOST, port=9090)
    if delete_table:
        try:
            connection.delete_table(HBASE_TABLE, disable=True)

        except:
            pass
    connection.create_table(HBASE_TABLE, {
         'family': dict(max_versions=10),
         'family': dict(max_versions=1, block_cache_enabled=False),
         'family': dict(),
	 'family': dict(),
         'family': dict()}
                            )
    connection.close()
    return True


def write_row(row):
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=9090)
        table_name = 'socialmedia'
        table = connection.table(table_name)
        row["family:date"]=bytes(datetime.datetime.now().strftime("%c"), 'utf-8')
        print(row["family:date"])
        table.put(uuid.uuid4().hex,row)
        connection.close()
        return True
    except:
        pass
    return False
