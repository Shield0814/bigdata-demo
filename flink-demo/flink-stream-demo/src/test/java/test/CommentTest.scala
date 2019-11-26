package test

import scala.util.Random

object CommentTest {

    def main(args: Array[String]): Unit = {
        val random = Random
        for (elem <- 1 to 100) {
            println(random.nextInt(1000))
        }

    }
}
