package Project;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HbaseCoding {

    private static Configuration configuration;
    private static Connection connection;
    private static Admin hAdmin;


    static {
        //初始化Hbase连接
        configuration=HBaseConfiguration.create();
        //加载配置文件
        configuration.addResource(Resources.getResource("hbase-site.xml"));

        try {
            connection = ConnectionFactory.createConnection(configuration);
            //create table
            hAdmin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    //main函数
    public static void main(String[] args) throws IOException {
        //Task 1:创建表
        System.out.println("Task 1:Create table 'students'");
        String[] ColumnFamily = {"Description", "Courses", "Home"};
        createTable("students", ColumnFamily);

        //向表中插入数据
        System.out.println("Now Insert students data");
        insert("students", "001", "Description", "Name", "Li Lei");
        insert("students", "001", "Description", "Height", "176");
        insert("students", "001", "Courses", "Chinese", "80");
        insert("students", "001", "Courses", "Math", "90");
        insert("students", "001", "Courses", "Physics", "95");
        insert("students", "001", "Home", "Province", "Zhejiang");

        insert("students", "002", "Description", "Name", "Han Meimei");
        insert("students", "002", "Description", "Height", "183");
        insert("students", "002", "Courses", "Chinese", "88");
        insert("students", "002", "Courses", "Math", "77");
        insert("students", "002", "Courses", "Physics", "66");
        insert("students", "002", "Home", "Province", "Beijing");

        insert("students", "003", "Description", "Name", "Xiao Ming");
        insert("students", "003", "Description", "Height", "162");
        insert("students", "003", "Courses", "Chinese", "90");
        insert("students", "003", "Courses", "Math", "90");
        insert("students", "003", "Courses", "Physics", "90");
        insert("students", "003", "Home", "Province", "Shanghai");
        System.out.println("Task 1 finished successfully!\n");

        //Task 2:扫描创建后的students表
        System.out.println("Task 2:Scan table 'students'");
        scanTable("students");
        System.out.println("Task 2 finished successfully!\n");

        //Task 3:查询学生来自的省
        System.out.println("Task 3:Select student's province");
        selectProvince("students", "001","Home","Province");
        selectProvince("students", "002","Home","Province");
        selectProvince("students", "003","Home","Province");
        System.out.println("Task 3 finished successfully!\n");

        //Task 4:增加新的列 Courses:English
        System.out.println("Task 4:Add new column 'Courses:English'");
        insert("students", "001", "Courses", "English", "95");
        insert("students", "002", "Courses", "English", "85");
        insert("students", "003", "Courses", "English", "98");
        System.out.println("Task 4 finished successfully!\n");

        //Task 5:增加新的列族 Contact 和列 Contact:Email
        System.out.println("Task 5:Add new ColumnFamily 'Contact' adn column 'Contact:Email'");
        HColumnDescriptor newColumnFamliy = new HColumnDescriptor("Contact");
        hAdmin.addColumn(TableName.valueOf("students"), newColumnFamliy);
        insert("students", "001", "Contact", "Email", "lilei@qq.com");
        insert("students", "002", "Contact", "Email", "hanmeimei@qq.com");
        insert("students", "003", "Contact", "Email", "xiaoming@qq.com");
        System.out.println("Task 5 finished successfully!\n");
        System.out.println("Show the renewed table\n");
        scanTable("students");

        //Task 6:删除students表
        System.out.println("Task 6:Drop table 'students'");
        dropTable("students");
        System.out.println("Task 6 finished successfully!");

    }




    //创建数据库表
    public static void createTable(String tableName,String[] ColumnFamily) throws IOException {
       //判断表是否存在
       if(hAdmin.tableExists(org.apache.hadoop.hbase.TableName.valueOf(tableName))) {
           System.out.println("Table "+tableName+" already exists!");
       }
       else{
           //新建一个表的描述
           HTableDescriptor TableDescriptor = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(tableName));
           //添加列族
           for(String cf : ColumnFamily){
               HColumnDescriptor tmp = new HColumnDescriptor(cf);
               TableDescriptor.addFamily(tmp);
           }
           hAdmin.createTable(TableDescriptor);
           System.out.println("Create table '"+tableName+"' successfully!");
       }
    }

    //删除数据库表
    public static void dropTable(String tableName) throws IOException{
        //判断表存不存在
        if(hAdmin.tableExists(org.apache.hadoop.hbase.TableName.valueOf(tableName))){
            // 先disable，再delete
            hAdmin.disableTable(TableName.valueOf(tableName));
            hAdmin.deleteTable(TableName.valueOf(tableName));
            System.out.println("Delete "+tableName+" Successfully！");
        } else {
            System.out.println(tableName+" doesn't exist！");
            System.exit(0);
        }
    }

    //插入数据
    public static void insert(String tableName,String row,String ColumnFamily,String column,String value)
        throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        //指定行
        Put put=new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(ColumnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        System.out.println("Insert data '"+row+" "+column+" "+value+"' successfully!");
        
    }
    
    

    //扫描表
    public static void scanTable(String tableName) throws IOException{
        //获得数据表对象
        Table table=connection.getTable(TableName.valueOf(tableName));
        //获取表中的数据
        ResultScanner results=table.getScanner(new Scan());
        for(Result result : results){
            byte[] row=result.getRow();
            System.out.println("Row key："+new String(row));
            List<Cell> listCells=result.listCells();
            for(Cell cell : listCells){
                //column family Array
                byte[] familyArray = cell.getFamilyArray();
                //column Array
                byte[] qualifierArray = cell.getQualifierArray();
                //value array
                byte[] valueArray = cell.getValueArray();
                long Timestamp=cell.getTimestamp();
                System.out.print("Row:"+new String(row)+"  "+
                        "Timestamp："+ Timestamp +", "
                        +"Column Family："+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength())+", "
                        +"Column："+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength())+", "
                        +"Value："+new String(valueArray,cell.getValueOffset(),cell.getValueLength())+"\n");
            }
        }
        System.out.println("Scan table "+tableName+"successfully!");
        
    }
    
    //查询
    public static void selectProvince(String tableName,String row,String ColumnFamily,String column) throws IOException{
        //获得数据表对象
        Table table=connection.getTable(TableName.valueOf(tableName));
        //新建一个查询对象作为查询条件
        Get get=new Get(row.getBytes());
        get.addColumn(ColumnFamily.getBytes(),column.getBytes());
        //按行查询数据
        Result result=table.get(get);
        List<Cell> listCells=result.listCells();
        for(Cell cell:listCells){
            byte[] valueArray = cell.getValueArray();
            System.out.println("Row key:"+row+", value:"+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
        }
        System.out.println("Select "+row+"'s Province successfully!");
    }

}
