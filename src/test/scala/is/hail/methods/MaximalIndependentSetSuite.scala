package is.hail.methods

import is.hail.SparkSuite
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.testng.annotations.Test

/**
  * Tests for MaximalIndependentSet
  */
class MaximalIndependentSetSuite extends SparkSuite {

  @Test def emptySet() {
    assert(MaximalIndependentSet(Graph(sc.parallelize(Array[(VertexId, Int)]()),
      sc.parallelize(Array[Edge[Int]]()))) == Set())
  }

  @Test def graphWithoutEdges() {
    val vertices = sc.parallelize(Array[(VertexId, String)](
      (1, "A"), (2, "B"), (3, "C")
    ))

    val edges = sc.parallelize(Array[Edge[Int]]())

    assert(MaximalIndependentSet(Graph(vertices, edges)) == Set(1, 2, 3))
  }

  @Test def simpleGraph() {
    val vertices = sc.parallelize(Array[(VertexId, String)](
      (1, "A"), (2, "B"), (3, "C")
    ))

    val edges = sc.parallelize(Array[Edge[String]](
      Edge(1, 2, "AB"), Edge(2, 3, "BC")
    ))

    assert(MaximalIndependentSet(Graph(vertices, edges), true) == Set(1, 3))
  }

  @Test def directedTest() {
    val vertices = sc.parallelize(Array[(VertexId, String)](
      (1, "A"), (2, "B"), (3, "C")
    ))

    val edges = sc.parallelize(Array[Edge[String]](
      Edge(1, 2, "AB"), Edge(1, 3, "BC")
    ))

    assert(MaximalIndependentSet(Graph(vertices, edges)) == Set(2, 3))
  }

  @Test def ofIBDMatrixTest1() {
    val input: RDD[((Int, Int), Double)] = sc.parallelize(Array(
      ((1, 2), 0.3), ((2, 3), 0.3), ((2, 4), 0.3),
      ((4, 6), 0.0), ((6, 5), 0.0), ((7, 6), 0.0),
      ((6, 8), 0.0)
    ))

    assert(MaximalIndependentSet.ofIBDMatrix(input, 0.8, 1 to 7) == Set(1, 3, 4, 5, 7, 8))
    assert(MaximalIndependentSet.ofIBDMatrix(input, 0.2, 1 to 7) == Set(1, 2, 3, 4, 5, 7, 8))
  }

  @Test def ofIBDMatrixTest2() {
    val input: RDD[((Int, Int), Double)] = sc.parallelize(Array(
      ((1, 2), .4), ((3, 4), .3)
    ))

    assert(MaximalIndependentSet.ofIBDMatrix(input, 0.5, 1 to 4).size == 2)
  }

  @Test def largeComponentWithTwoSubComponents() {
    val input: RDD[((Int, Int), Double)] = sc.parallelize(Array(
      (1, 2), (1, 3), (2, 4), (2, 5), (3, 4), (3, 5),
      (8, 6), (8, 7), (6, 4), (6, 5), (7, 4), (7, 5)
    ).map(x => (x, 1.0)))

    MaximalIndependentSet.ofIBDMatrix(input, 0.8, 1 to 8) == (Set(2, 3, 6, 7))
  }
}
