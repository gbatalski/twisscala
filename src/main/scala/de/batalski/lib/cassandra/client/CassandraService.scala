/**
 *
 */
package de.batalski.lib.cassandra.client
import scala.collection.JavaConversions._
import me.prettyprint.cassandra.service.CassandraHostConfigurator
import me.prettyprint.cassandra.serializers.StringSerializer
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.ddl.KeyspaceDefinition
import net.liftweb.common._
import me.prettyprint.cassandra.service.ThriftCfDef
import me.prettyprint.hector.api.ddl.ColumnType._
import me.prettyprint.hector.api.query.QueryResult
import scala.collection.immutable.HashMap
import me.prettyprint.hector.api.beans.HColumn
import scala.collection.immutable.TreeMap
/**
 * @author gena
 *
 */
class CassandraService (val host: String, val port: Int, val clusterName: String, val keyspace: String) extends Logger{
  
  def this() = this("127.0.0.1", 9160, "Twisscala Cluster", "twisscala")

  val CASSANDRA_THRIFT_SOCKET_TIMEOUT = 3000
  val MAXWAITTIMEWHENEXHAUSTED = 4000

  val MAX_ROW_COUNT = 1000
  val MAX_COLUMN_COUNT = 1000
  val SE = StringSerializer.get()
  
  
  
  val cassandraHostConfigurator = new CassandraHostConfigurator()
  cassandraHostConfigurator.setHosts(host)
  cassandraHostConfigurator.setPort(port)
  cassandraHostConfigurator.setMaxActive(100)
  cassandraHostConfigurator.setMaxIdle(10)
  cassandraHostConfigurator.setCassandraThriftSocketTimeout(CASSANDRA_THRIFT_SOCKET_TIMEOUT)
  cassandraHostConfigurator.setMaxWaitTimeWhenExhausted(MAXWAITTIMEWHENEXHAUSTED)
  cassandraHostConfigurator.setRetryDownedHosts(true)

  val _cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator)
  createKeyspaceIfAbsent(keyspace)
  val _keyspace = HFactory.createKeyspace(keyspace, _cluster)

  
  
  /**
     *
     * @param keyspace
     */
   def createKeyspaceIfAbsent(keyspace:String) =
        try {
            val kdef = HFactory.createKeyspaceDefinition(keyspace,
                    "org.apache.cassandra.locator.SimpleStrategy", 1, null);

            _cluster.addKeyspace(kdef);
        } catch {
            //ignore
          case ex => warn(ex.getMessage(), ex);
        }
    

    /**
     * create column family if absent, not super man.
     *
     * @param columnFamily
     */
    def createColumnFamilyIfAbsent(columnFamily:String) = 

        try {
            val colDef = HFactory.createColumnFamilyDefinition(_keyspace.getKeyspaceName(),
                    columnFamily)
            _cluster.addColumnFamily(colDef)
        } catch {
          case ex => warn(ex.getMessage(), ex)
        }
    


    /**
     * create super column family if absent
     *
     * @param columnFamily
     */
    def createSuperColumnFamilyIfAbsent(columnFamily:String) {
        try {
            val cfDef =  HFactory.createColumnFamilyDefinition(_keyspace.getKeyspaceName(), columnFamily).asInstanceOf[ThriftCfDef]
            cfDef.setColumnType(SUPER)

            _cluster.addColumnFamily(cfDef);
        } catch  {
          case ex => warn(ex.getMessage(), ex);
        }
    }

    /**
     * read the value from the cassandra
     *
     * @param key
     * @param columnName
     * @param columnFamily
     * @return
     */
    def readColumn(key:String, columnName:String, columnFamily:String ):String =
                             {
        val columnQuery = HFactory.createStringColumnQuery(_keyspace)
        columnQuery.setColumnFamily(columnFamily).setKey(key).setName(columnName)
        val result = columnQuery.execute()
        val column = result.get()

        if (null == column)
            ""
        else
        column.getValue()
     }

    /**
     * read a column from the super column
     *
     * @param key
     * @param columnName
     * @param superColumn
     * @param columnFamily
     * @return
     */
    def readSubColumn(key: String, columnName: String, superColumn :String, columnFamily: String ) : String  =
    {

        val subColumnQuery = HFactory.createSubColumnQuery(_keyspace,SE, SE, SE, SE)
        subColumnQuery.setKey(key).setColumn(columnName).setSuperColumn(superColumn).setColumnFamily(columnFamily)

        val result = subColumnQuery.execute()
        val column = result.get()

        if (null == column) 
          ""
        else
        	column.getValue()
    }

    /**
     * read multi columns
     *
     * @param key
     * @param columns
     * @param columnFamily
     * @return
     */
    def readColumns(key: String, columns: Array[String],  columnFamily:String): Map[String, String] =
    {
        val results = new HashMap[String, String]()

        val sliceQuery = HFactory.createSliceQuery(_keyspace, SE, SE, SE)
        sliceQuery.setColumnFamily(columnFamily).setKey(key).setColumnNames(columns:_*)

        val columnSlice = sliceQuery.execute().get()

        for ( column <- columns) {
            def hColumn = columnSlice.getColumnByName(column)
            if (null == hColumn) {
                results + ((column, ""))
            } else {
                results + ((column, hColumn.getValue()))
            }
        }

        results
    }

    /**
     * list columns in a column family
     *
     * return  MAX_COLUMN_COUNT columns at most
     *
     * @param key
     * @param columnFamily
     * @return
     */
    def listColumns(key:String,columnFamily:String) : Map[String, String] = {

         listColumns(key, columnFamily, null, MAX_COLUMN_COUNT)
    }

    def listColumns(key:String ,columnFamily:String,startColumn:String) :  Map[String, String] = {

         listColumns(key, columnFamily, startColumn, MAX_COLUMN_COUNT)
    }

    def listColumns(key:String,columnFamily:String, startColumn:String, count:Int): Map[String, String] = {
        val results = new HashMap[String, String]
        val sliceQuery = HFactory.createRangeSlicesQuery(_keyspace, SE, SE, SE)
        sliceQuery.setColumnFamily(columnFamily).setRange(startColumn, null, false, count)
                .setKeys(key, key).setRowCount(1);

        val rows = sliceQuery.execute().get();

        if (null == rows || 0 == rows.getCount()) {
            return results;
        }

        val columnSlice = rows.getList().get(0).getColumnSlice();

        if (null == columnSlice) {
            return results;
        }

        for ( hColumn <- asScalaBuffer(columnSlice.getColumns())) {
            results.put(hColumn.getName(), hColumn.getValue());
        }

        return results;
    }

    /**
     * read the columns in a super column.
     *
     * @param key
     * @param columns
     * @param superColumn
     * @param columnFamily
     * @return
     */
    def readSubColumns(key:String,columns: Array[String],superColumn:String,columnFamily:String):Map[String, String] = {
       val results = new HashMap[String, String]

        val subSliceQuery = HFactory.createSubSliceQuery(_keyspace, SE, SE, SE, SE)
        subSliceQuery.setColumnFamily(columnFamily).setSuperColumn(superColumn).setKey(key).setColumnNames(columns:_*)

       val columnSlice = subSliceQuery.execute().get()

        for (column <- columns) {
           val  hColumn = columnSlice.getColumnByName(column)
            if (null == hColumn) {
                results.put(column, "")
            } else {
                results.put(column, hColumn.getValue())
            }
        }

        results
    }

    /**
     * read a super column
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @return
     */
    def readSuperColumns(key:String, superColumn:String,columnFamily:String):Map[String,String]={
        val results = new HashMap[String, String]

        val superColumnQuery = HFactory.createSuperColumnQuery(_keyspace,SE, SE, SE, SE)
        
        superColumnQuery.setKey(key).setSuperName(superColumn).setColumnFamily(columnFamily)

        val sColumn = superColumnQuery.execute().get()

        if (null == sColumn) {
            return results
        }

        for (column <- sColumn.getColumns()) {
            results.put(column.getName(), column.getValue());
        }
        results;
    }

    /**
     * update the value in the column
     *
     * @param key
     * @param value
     * @param columnName
     * @param columnFamily
     */
    def updateColumn(key:String ,value:String ,columnName:String ,columnFamily:String )
    {
        val mutator = HFactory.createMutator(_keyspace, SE)

        // insert (row, column family, column(key, value));
        mutator.insert(key, columnFamily, HFactory.createStringColumn(columnName, value))
    }

    /**
     * update the column in a super column
     *
     * @param key
     * @param value
     * @param columnName
     * @param superColumn
     * @param columnFamily
     */
    def updateSubColumn(key:String ,value:String ,columnName:String ,superColumn:String ,columnFamily:String )
    {
        val mutator = HFactory.createMutator(_keyspace, SE)

        // insert (row, columnfamily, column(key, value));
        mutator.insert(key, columnFamily, HFactory.createSuperColumn(superColumn, 
                                                                   java.util.Arrays.asList( HFactory.createStringColumn(columnName, value)), SE, SE, SE))
    }

    /**
     *
     * update the columns in a super column
     *
     * @param key
     * @param columns
     * @param superColumn
     * @param columnFamily
     */
    def updateSubColumns(key:String,columns:Map[String,String],superColumn:String,columnFamily:String)
    {
        val mutator = HFactory.createMutator(_keyspace, SE)

        val columnList = List[HColumn[String, String]]()
        
        for ( columnName <-columns.keys) {
            columnList.add(HFactory.createStringColumn(columnName, columns.get(columnName).get));
        }

        mutator.insert(key, columnFamily, HFactory.createSuperColumn(superColumn,
                columnList, SE, SE, SE));
    }

    /**
     * delete the column
     *
     * @param key
     * @param columnName
     * @param columnFamily
     */
    def deleteColumn(key:String,columnName:String,columnFamily:String)
    {
       val mutator = HFactory.createMutator(_keyspace, SE);

        mutator.delete(key, columnFamily, columnName, SE);
    }

    /**
     * delete the column in a super column
     *
     * @param key
     * @param columnName
     * @param superColumn
     * @param columnFamily
     */
    def deleteSubColumn(key:String,columnName:String,superColumn:String,columnFamily:String)
    {
        val mutator = HFactory.createMutator(_keyspace, SE)

        mutator.subDelete(key, columnFamily, superColumn, columnName, SE, SE)
    }

    /**
     * return all the keys
     *
     * @param columnFamily
     * @return
     */
    def listKeys(columnFamily:String ):List[String] = {
        val rangeSlicesQuery = HFactory.createRangeSlicesQuery(_keyspace, SE, SE, SE)

        rangeSlicesQuery.setColumnFamily(columnFamily).setReturnKeysOnly().setRowCount(MAX_ROW_COUNT)
        
        val rows = rangeSlicesQuery.execute().get()


        val result = List[String]()

        // return empty list
        if (null == rows) {
            return result;
        }

        for ( row <- rows.getList()) {
            result.add(row.getKey());
        }

        if (result.size() < MAX_ROW_COUNT) {
            return result;
        }

        var startKey = rows.peekLast().getKey();
        var next = listKeys(columnFamily, startKey);

        while(next.size() != 0) {

            result.addAll(next);
            startKey = next.get(next.size() - 1);
            next = listKeys(columnFamily, startKey);
        }
        return result;
    }

    /**
     * return the keys after the startKey
     *
     * @param columnFamily
     * @param startKey
     * @return
     */
    def listKeys(columnFamily:String,startKey:String):List[String] = {
        val rangeSlicesQuery = HFactory.createRangeSlicesQuery(_keyspace, SE, SE, SE);

        rangeSlicesQuery.setColumnFamily(columnFamily).setReturnKeysOnly().setRowCount(MAX_ROW_COUNT + 1).setKeys(startKey, null)
        val rows = rangeSlicesQuery.execute().get()

        val result = List[String]()

        // return empty list
        if (null == rows) {
            return result
        }

        for (row <- rows.getList().filterNot(_.getKey().equals(startKey))){
            result.add(row.getKey())
        }
        return result
    }

    /**
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @return
     */
   def listSubColumns(key:String,superColumn:String,columnFamily:String): Map[String,String] = {
        listSubColumns(key, superColumn, columnFamily, true)
    }


    /**
     * list the columns in a super column.
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @param reversed
     * @return
     */
    def listSubColumns(key:String,superColumn:String,columnFamily:String,reversed:Boolean): Map[String,String] = {
       val result = TreeMap[String, String]()

       val rangeSubSlicesQuery = HFactory.createRangeSubSlicesQuery(_keyspace, SE, SE, SE, SE)

        rangeSubSlicesQuery.setColumnFamily(columnFamily).setKeys(key, key)
                .setRange(null, null, reversed, MAX_COLUMN_COUNT)
                .setSuperColumn(superColumn)

        val rows = rangeSubSlicesQuery.execute().get();

        if (null == rows || 0 == rows.getCount()) {
            return result
        }

        // get the row
        val row = rows.getList().get(0)
        val columns = row.getColumnSlice()

        if (null == columns) {
            return result
        }

        for (column <- columns.getColumns()) {
            result.put(column.getName(), column.getValue());
        }

        return result;
    }

    /**
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @param startColumn
     * @return
     */
    def listSubColumns(key:String, superColumn:String, columnFamily:String, startColumn:String):Map[String,String] =  {
        listSubColumns(key, superColumn, columnFamily, startColumn, true)

    }

    /**
     *
     * list columns start from a startcolumn.
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @param startColumn
     * @param reversed
     * @return
     */
    def listSubColumns(key:String, superColumn:String, columnFamily:String, startColumn:String,
                                              reversed:Boolean): Map[String,String] ={

        /*if (StringHelper.IsNullOrEmpty(startColumn)) {
            return listSubColumns(key, superColumn, columnFamily);
        }*/

       val result = TreeMap[String, String]()

        val rangeSubSlicesQuery = HFactory.createRangeSubSlicesQuery(_keyspace, SE, SE, SE, SE)

        rangeSubSlicesQuery.setColumnFamily(columnFamily).setKeys(key, key)
                .setRange(startColumn, null, reversed, MAX_COLUMN_COUNT + 1)
                .setSuperColumn(superColumn)

        val rows = rangeSubSlicesQuery.execute().get()

        if (null == rows || 0 == rows.getCount()) {
            return result
        }

        // get the row
        val row = rows.getList().get(0)
        val columns = row.getColumnSlice()

        if (null == columns) {
            return result
        }

        for (column <- columns.getColumns().filterNot(_.getName().equals(startColumn))) {
            result.put(column.getName(), column.getValue())
        }

        return result;
    }

    /**
     * count the columns for a key in a super column.
     *
     * @param key
     * @param superColumn
     * @param columnFamily
     * @return
     */
    def countSubColumns(key:String,superColumn:String,columnFamily:String ):Int = {

        val subCountQuery = HFactory.createSubCountQuery(_keyspace, SE, SE, SE);
        subCountQuery.setColumnFamily(columnFamily).setKey(key).setSuperColumn(superColumn).setRange(null, null,
                Integer.MAX_VALUE)

        subCountQuery.execute().get()
        
    }
  
}