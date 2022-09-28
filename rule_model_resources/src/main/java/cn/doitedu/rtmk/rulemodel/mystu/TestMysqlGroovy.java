package cn.doitedu.rtmk.rulemodel.mystu;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import groovy.lang.Tuple2;

import java.sql.*;

public class TestMysqlGroovy {
    public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:53307/groovytest", "root", "example");


        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select rule_str from groovy_java");

        resultSet.next();
        String groovyCodeStr = resultSet.getString("rule_str");

        //
        GroovyClassLoader classLoader = new GroovyClassLoader();

        // 解析源代码，编译成class
        Class groovyClass = classLoader.parseClass(groovyCodeStr);
        TestGroovy sum_test = (TestGroovy) groovyClass.newInstance();

        Tuple2<Integer, Integer> tuple2 = Tuple2.tuple(1, 3);

        System.out.println(tuple2.getV1());

        int asd = sum_test.getSum(tuple2);
        System.out.println(asd);


        System.out.println("在java中代码中，打印 groovy代码调用后的返回值： " + asd);

    }
}
