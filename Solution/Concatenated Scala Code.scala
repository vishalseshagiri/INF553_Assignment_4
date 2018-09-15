// Task 1
// Betweenness.scala

import org.apache.spark.{SparkConf, SparkContext}
import System.currentTimeMillis
import java.io.PrintWriter
import java.io.File

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue

object Betweenness {

  var dict_adj_matrix = Map[Int, Set[Int]]()
  var new_adj_matrix = collection.mutable.Map[Int, collection.mutable.Set[Int]]()
  var communities_list = new ListBuffer[collection.mutable.Set[Int]]()
  var number_of_edges : Int = 0
  var modularity_community_dict = collection.mutable.Map[Int, Float]()

  def get_movie_rating_counts(scalaIterator : Iterator[(Int, Iterable[Int])]) : Iterator[(Tuple2[Int, Int], Int)] = {
    var movies = scalaIterator.map(row => row._1 -> row._2).toMap
    var returnIterator = new ListBuffer[(Tuple2[Int, Int], Int)]

    movies.keys.foreach((movie) => {
      var userListSorted = movies(movie).toList.sorted

      userListSorted.combinations(2).foreach({
        pair => returnIterator += (((pair(0), pair(1)), 1))
      })
    })

    return returnIterator.toIterator
  }

  def generate_adj_matrix(scalaTuple : (Int, Int)) : Iterator[(Int, Int)]= {
    var returnIterator = new ListBuffer[(Int, Int)]
    returnIterator.++=:(Iterator(scalaTuple))
    returnIterator.++=:(Iterator((scalaTuple._2, scalaTuple._1)))

    return returnIterator.toIterator
  }

  def bfs_new(node : Int) : collection.mutable.Map[String, Float] = {

    var visited_order_set = collection.mutable.Set(node)
    var visit_order = new ListBuffer[Int]()
    var seen = collection.mutable.Map[Int, (ListBuffer[Int], Int)]()
    var to_visit = new Queue[Int]()
    var to_visit_set = collection.mutable.Set(node)
    to_visit += node
    to_visit_set += node

    while(!to_visit.isEmpty) {
      var vertex = to_visit.dequeue()
      to_visit_set.remove(vertex)

      if (vertex == node) {
        seen(node) = (ListBuffer[Int](), 0)
        visit_order += node
        visited_order_set += node
      }
      else {
        visit_order += vertex
        visited_order_set += vertex
      }

      var vertex_distance_from_root = seen(vertex)._2

      // add predecessors in seen

      var to_be_appended = dict_adj_matrix(vertex).diff(visited_order_set)

      var to_be_visited = to_be_appended.diff(to_visit_set)
      if (!to_be_visited.isEmpty) {
        for (n <- to_be_visited) {
          to_visit_set += n
          to_visit += n
        }
      }

      for (item <- to_be_appended) {
        if (seen.get(item) == None) {
          seen(item) = (ListBuffer[Int](vertex), seen(vertex)._2 + 1)
        }
        else {
          if (seen(item)._2 == vertex_distance_from_root + 1) {
            seen(item)._1 += vertex
          }
        }
      }
    }

    // addition(calculating the betweenness of the edges in the bfs_new function)
    var temp_nodes = collection.mutable.Map[Int, Float]()
    var temp_edges = collection.mutable.Map[String, Float]()

    for (inner_node <- visit_order.reverse) {
      var inner_node_value = temp_nodes.getOrElse(inner_node, 0.toFloat)
      temp_nodes(inner_node) = if (inner_node_value != 0.toFloat) inner_node_value + 1.toFloat else 1.toFloat
      var ends = seen(inner_node)._1
      var current_inner_node_value = temp_nodes(inner_node)
      var number_of_influenced_nodes = ends.size
      var influencing_value = 0.toFloat
      if (number_of_influenced_nodes != 0) {
        influencing_value = current_inner_node_value / number_of_influenced_nodes
      }
      for (end <- ends) {
        var end_node_value = temp_nodes.getOrElse(end, -1.toFloat)
        var sorted_edge_list = List(inner_node, end).sorted
        var edge_name : String = s"${sorted_edge_list(0)} ${sorted_edge_list(1)}"
        temp_edges(edge_name) = influencing_value
        temp_nodes(end) = if (end_node_value != -1.toFloat) end_node_value + influencing_value else influencing_value
      }

    }

    return temp_edges

  }

  def girvan_newman(node : Int) : Iterator[(String, Float)] = {
    return bfs_new(node).map(e => (e._1, e._2)).toIterator
  }

  def main(arg : Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("GirvanNewman Assignment 4")
      .setMaster("local[2]")

    val case_number = 2
    val sc = new SparkContext(conf)

    val ratings_path = arg(0)

    var ml_latest_small = sc.textFile(ratings_path)

    val header = ml_latest_small.first()

    var adj_matrix = ml_latest_small
      .filter(row => row != header)
      .map(row => {
        row.split(",").slice(0, 2).reverse.map(_.toInt) match {
          case Array(a, b) => (a, b)
        }
      })
      .groupByKey()
      .mapPartitions(get_movie_rating_counts)
      .groupByKey()
      .mapValues(row => row.size)
      .filter(row => row._2 >= 9)
      .map(row => row._1)
      .flatMap(row => generate_adj_matrix(row))
      .groupByKey()
      .map(row => {
        (row._1, Set(row._2.toSet.toSeq:_*))
      })
      .sortByKey()
      .collect()

    dict_adj_matrix = adj_matrix.toMap

    var nodes = dict_adj_matrix.keys

    var parallelized_nodes = sc.parallelize(nodes.toSeq, 10)

    var output_list_of_tuples = parallelized_nodes
      .map(x => girvan_newman(x))
      .flatMap(x => x)
      .groupByKey()
      .mapValues(x => x.sum / 2.toFloat)
      .map(row => (row._1.split(" ").map(_.toInt) match {
        case Array(a, b) => (a, b)
      }, row._2))
      .sortByKey()

    var sorted_tuples = output_list_of_tuples.collect()
//    var sorted_tuples = output_list_of_tuples.collect().sortWith((a, b) => {
//      a._1._1 < b._1._1 || a._1._1 == b._1._1 && a._1._2 < b._1._2
//    })


    val pw = new PrintWriter(new File("Vishal_Seshagiri_Betweenness_Scala.txt"))

    sorted_tuples.foreach((tuple) => {
      var line = s"${tuple._1._1},${tuple._1._2},${tuple._2}"
      pw.write(s"(${line})\n")
    })

    pw.close()

  }


}


// Task 2
// Community.scala

import org.apache.spark.{SparkConf, SparkContext}
import System.currentTimeMillis
import java.io.PrintWriter
import java.io.File

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue

object Community {

  var dict_adj_matrix = Map[Int, Set[Int]]()
  var new_adj_matrix = collection.mutable.Map[Int, collection.mutable.Set[Int]]()
  var communities_list = new ListBuffer[collection.mutable.Set[Int]]()
  var number_of_edges : Int = 0
  var modularity_community_dict = collection.mutable.Map[Int, Float]()

  def get_movie_rating_counts(scalaIterator : Iterator[(Int, Iterable[Int])]) : Iterator[(Tuple2[Int, Int], Int)] = {
    var movies = scalaIterator.map(row => row._1 -> row._2).toMap
    var returnIterator = new ListBuffer[(Tuple2[Int, Int], Int)]

    movies.keys.foreach((movie) => {
      var userListSorted = movies(movie).toList.sorted

      userListSorted.combinations(2).foreach({
        pair => returnIterator += (((pair(0), pair(1)), 1))
      })
    })

    return returnIterator.toIterator
  }

  def generate_adj_matrix(scalaTuple : (Int, Int)) : Iterator[(Int, Int)]= {
    var returnIterator = new ListBuffer[(Int, Int)]
    returnIterator.++=:(Iterator(scalaTuple))
    returnIterator.++=:(Iterator((scalaTuple._2, scalaTuple._1)))

    return returnIterator.toIterator
  }

  def bfs_new(node : Int) : collection.mutable.Map[String, Float] = {

    var visited_order_set = collection.mutable.Set(node)
    var visit_order = new ListBuffer[Int]()
    var seen = collection.mutable.Map[Int, (ListBuffer[Int], Int)]()
    var to_visit = new Queue[Int]()
    var to_visit_set = collection.mutable.Set(node)
    to_visit += node
    to_visit_set += node

    while(!to_visit.isEmpty) {
      var vertex = to_visit.dequeue()
      to_visit_set.remove(vertex)

      if (vertex == node) {
        seen(node) = (ListBuffer[Int](), 0)
        visit_order += node
        visited_order_set += node
      }
      else {
        visit_order += vertex
        visited_order_set += vertex
      }

      var vertex_distance_from_root = seen(vertex)._2

      // add predecessors in seen

      var to_be_appended = dict_adj_matrix(vertex).diff(visited_order_set)

      var to_be_visited = to_be_appended.diff(to_visit_set)
      if (!to_be_visited.isEmpty) {
        for (n <- to_be_visited) {
          to_visit_set += n
          to_visit += n
        }
      }

      for (item <- to_be_appended) {
        if (seen.get(item) == None) {
          seen(item) = (ListBuffer[Int](vertex), seen(vertex)._2 + 1)
        }
        else {
          if (seen(item)._2 == vertex_distance_from_root + 1) {
            seen(item)._1 += vertex
          }
        }
      }
    }

    // addition(calculating the betweenness of the edges in the bfs_new function)
    var temp_nodes = collection.mutable.Map[Int, Float]()
    var temp_edges = collection.mutable.Map[String, Float]()

    for (inner_node <- visit_order.reverse) {
      var inner_node_value = temp_nodes.getOrElse(inner_node, 0.toFloat)
      temp_nodes(inner_node) = if (inner_node_value != 0.toFloat) inner_node_value + 1.toFloat else 1.toFloat
      var ends = seen(inner_node)._1
      var current_inner_node_value = temp_nodes(inner_node)
      var number_of_influenced_nodes = ends.size
      var influencing_value = 0.toFloat
      if (number_of_influenced_nodes != 0) {
        influencing_value = current_inner_node_value / number_of_influenced_nodes
      }
      for (end <- ends) {
        var end_node_value = temp_nodes.getOrElse(end, -1.toFloat)
        var sorted_edge_list = List(inner_node, end).sorted
        var edge_name : String = s"${sorted_edge_list(0)} ${sorted_edge_list(1)}"
        temp_edges(edge_name) = influencing_value
        temp_nodes(end) = if (end_node_value != -1.toFloat) end_node_value + influencing_value else influencing_value
      }

    }

    return temp_edges

  }

  def girvan_newman(node : Int) : Iterator[(String, Float)] = {
    return bfs_new(node).map(e => (e._1, e._2)).toIterator
  }

  def bfs_community(communities_list : ListBuffer[collection.mutable.Set[Int]], edge_to_remove : Tuple2[Int, Int]) : (ListBuffer[collection.mutable.Set[Int]], Int, Float) = {
    var nodes_ = edge_to_remove
    var set_nodes = nodes_.productIterator.toSet

    new_adj_matrix(nodes_._1).remove(nodes_._2)
    new_adj_matrix(nodes_._2).remove(nodes_._1)

    // optimization 1
    var (smaller, tofind) = if(new_adj_matrix(nodes_._1).size < new_adj_matrix(nodes_._2).size) {
      (new_adj_matrix(nodes_._1), nodes_._2)
    }
    else {
      (new_adj_matrix(nodes_._2), nodes_._1)
    }
    for (i <- smaller) {
      if (new_adj_matrix(i).contains(tofind)){
        return (communities_list, communities_list.size, -2.toFloat)
      }
    }

    var visited_ = ListBuffer[Int]()
    var visited_set = collection.mutable.Set[Int]()
    var to_visit = new Queue[Int]()
    var to_visit_set = collection.mutable.Set[Int]()
    to_visit += nodes_._2
    to_visit_set += nodes_._2

    while(!to_visit.isEmpty) {
      var vertex = to_visit.dequeue()
      to_visit_set.remove(vertex)

      visited_ += vertex
      visited_set += vertex

      var to_be_appended = new_adj_matrix(vertex).diff(visited_set)

      var to_be_visited = to_be_appended.diff(to_visit_set)

      for (n <- to_be_visited) {
        to_visit_set += n
        to_visit += n
      }
    }

    if (visited_set.contains(nodes_._1)) {
      // -2 is returned here to signify that no new community was formed
      return (communities_list, communities_list.size, -2.toFloat)
    }
    else {
      var new_communities_list = ListBuffer[collection.mutable.Set[Int]]()

      var break_point = -1

      var temp : Float = 0.toFloat

      var postponed_mods = ListBuffer[Float]()

      for((community, j) <- communities_list.view.zipWithIndex) {
        if (break_point == -1 && set_nodes.subsetOf(community.toSet)) {
          break_point = j

          var two_communities = ListBuffer[collection.mutable.Set[Int]]()
          two_communities += visited_set
          two_communities += community.diff(visited_set)

          new_communities_list.++=(two_communities)

          var count = 1

            for (p_community <- two_communities) {
              var comm_modularity = 0.toFloat
              //            print(s"before ${comm_modularity} ")

              var combinations = p_community.toList.combinations(2)
              for (node_pair <- combinations) {

                var a_ij = if (dict_adj_matrix(node_pair(1)).contains(node_pair(0))) 1.toFloat else 0.toFloat
                var ki = dict_adj_matrix(node_pair(0)).size.toFloat
                var kj = dict_adj_matrix(node_pair(1)).size.toFloat

                comm_modularity += ((a_ij - (ki * kj) / (2.toFloat * number_of_edges.toFloat)) * 2.toFloat)
              }
              postponed_mods += comm_modularity
            }
        }

        else {
          new_communities_list += community
          if (break_point != -1) {
            if (j == break_point + 1) {
              temp = modularity_community_dict(j)
            }
            else {
              var temp_temp = modularity_community_dict(j)
              modularity_community_dict(j) = temp
              temp = temp_temp
            }
            if (j == communities_list.size - 1 && j != break_point) {
              modularity_community_dict(j+1) = temp
            }
          }
        }
      }

      if (break_point == -1) {
        return (communities_list, communities_list.size, -2.toFloat)
      }

      modularity_community_dict(break_point) = postponed_mods(0)
      modularity_community_dict(break_point + 1) = postponed_mods(1)

      val return_modularity = modularity_community_dict.values.sum / (2.toFloat * number_of_edges.toFloat)
      return (new_communities_list, new_communities_list.size, return_modularity)
    }


  }

  def main(arg : Array[String]) : Unit = {
    val conf = new SparkConf()
      .setAppName("GirvanNewman Assignment 4")
      .setMaster("local[2]")

    val case_number = 2
    val sc = new SparkContext(conf)

    val ratings_path = arg(0)

    var ml_latest_small = sc.textFile(ratings_path)

    val header = ml_latest_small.first()

    var adj_matrix = ml_latest_small
      .filter(row => row != header)
      .map(row => {
        row.split(",").slice(0, 2).reverse.map(_.toInt) match {
          case Array(a, b) => (a, b)
        }
      })
      .groupByKey()
      .mapPartitions(get_movie_rating_counts)
      .groupByKey()
      .mapValues(row => row.size)
      .filter(row => row._2 >= 9)
      .map(row => row._1)
      .flatMap(row => generate_adj_matrix(row))
      .groupByKey()
      .map(row => {
        (row._1, Set(row._2.toSet.toSeq:_*))
      })
      .sortByKey()
      .collect()

    dict_adj_matrix = adj_matrix.toMap

    var nodes = dict_adj_matrix.keys

    var parallelized_nodes = sc.parallelize(nodes.toSeq, 10)

    var output_list_of_tuples = parallelized_nodes
      .map(x => girvan_newman(x))
      .flatMap(x => x)
      .groupByKey()
      .mapValues(x => x.sum / 2.toFloat)
      .map(row => (row._1.split(" ").map(_.toInt) match {
        case Array(a, b) => (a, b)
      }, row._2))
      .sortBy((row) => row._2)

    var edge_sorted_by_betweenness : ListBuffer[Tuple2[Int, Int]] = output_list_of_tuples
      .map(_._1)
      .collect()
      .reverse
      .to[ListBuffer]

    communities_list += collection.mutable.Set(dict_adj_matrix.keys.toSet.toSeq:_*)
    var number_of_communities = 0
    number_of_edges = edge_sorted_by_betweenness.size
    var modularity_dict = collection.mutable.Map[Tuple2[Int, Int], Float]()
    var max_modularity = -2.toFloat
    var max_modularity_communities = new ListBuffer[collection.mutable.Set[Int]]()
    dict_adj_matrix.foreach((n) => {
      new_adj_matrix(n._1) = collection.mutable.Set(n._2.toSeq:_*)
    })

    var count = 0
    for (edge <- edge_sorted_by_betweenness) {
      var return_result = bfs_community(communities_list, edge)
      communities_list = return_result._1
      number_of_communities = return_result._2
      var modularity_value = return_result._3

      // here we assume that if no community is being formed by removing that edge the value of modularity returned is -2.toFloat because Modularity lies between -1 and 1
      if (modularity_value != -2.toFloat) {
        count += 1
        modularity_dict(edge) = modularity_value

        if (max_modularity < modularity_value) {
          max_modularity = modularity_value
          max_modularity_communities = communities_list
        }
      }
    }

    var sorted_communities = max_modularity_communities.map((community) => {
      ListBuffer(community.toList.sorted:_*)
    }).sortWith((a, b) => a(0) < b(0))

    val pw = new PrintWriter(new File("Vishal_Seshagiri_Community_Scala.txt"))

    sorted_communities.foreach((community) => {
      var line = community.mkString(",")
      pw.write(s"[${line}]\n")
    })

    pw.close()

  }

}