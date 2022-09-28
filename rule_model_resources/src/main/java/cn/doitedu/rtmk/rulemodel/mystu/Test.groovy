package cn.doitedu.rtmk.rulemodel.mystu

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

class Test {
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
        GroovyObject sum_test = (GroovyObject) groovyClass.newInstance();

        Tuple2<Integer,Integer> tuple2=Tuple2.of(1,3);

        System.out.println(tuple2.getV1());

//        sum_test.invokeMethod("getSum",)
        int asd=(int)sum_test.invokeMethod("getSum",tuple2);
        System.out.println(asd);


        System.out.println("在java中代码中，打印 groovy代码调用后的返回值： " + asd);

    }
}
