package com.ljl.scala.exercise

object IOExercise {

    def main(args: Array[String]): Unit = {

        import collection.mutable.Set
        val threads = Set[String]()
        (1 to 100).foreach(x => {
            new Thread(new Runnable {
                override def run(): Unit = {
                    println((1 to 10000).reduce((x, y) => {
                        threads.add(Thread.currentThread.getName)
                        x + y
                    }))
                }
            }).start()
        })
        "".r

        //        val path = "d:/data/emp.csv"
        //        val file = Source.fromFile(path)
        //        val reverseLines = file.getLines().toArray.reverse
        //        reverseLines.foreach(println(_))
        //        val pw = new PrintWriter(path)
        //        reverseLines.foreach (line => pw.write(line+"\n"))
        //        pw.close()

    }
}
