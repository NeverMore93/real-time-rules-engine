package cn.doitedu.rtmk.rulemodel.mystu

class Test1 implements TestGroovy {
    static void main(String[] args) {
        def x = 89
        int s = 80
        print("数据信息{0}" + x + s)
        print("hello")


        // Initializing 3 variables
        def x1 = 5;
        def y = 10;
        def z = 8;

        //Performing addition of 2 operands
        println(x + y);
        def range = 5..10;
        println(range);
        println(range.get(3));
        Tuple2<Integer, Integer> tuple2 = Tuple2.tuple(1, 3)

        int asd = getSum(tuple2)
        println(asd)
        Stu stu = new Stu()
        stu.id = 10
        stu.name = 'tom'
        print(stu.getId() + stu.toString())
    }


//    def int getSum(Tuple2 tuple2) {
//        int a = tuple2.v1
//        int b = tuple2.v2
//        print("=============")
//        return a + b;
//    }

    @Override
    int getSum(Tuple2<Integer, Integer> tuple2) {
        int a = tuple2.v1
        int b = tuple2.v2
        print("=============")
        return a + b;
    }
}
